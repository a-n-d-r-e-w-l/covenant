use std::{
    collections::HashSet,
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use anyhow::Context;
use memmap2::Mmap;
use tokio::{
    io::{AsyncRead, AsyncWriteExt},
    pin,
    sync::RwLock,
};

mod hashes;
mod lock;
mod token;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ObjectId(NonZeroU64);

#[derive(Debug)]
pub struct Ark {
    paths: Pather,
    data_lock: lock::Lock,
    objects_lock: lock::Lock,
    inner: RwLock<Inner>,
}

impl Ark {
    pub async fn open(data_dir: &Path, object_dir: &Path) -> anyhow::Result<Self> {
        let paths = Pather::new(data_dir, object_dir);
        if !data_dir.exists() {
            fs_err::tokio::create_dir_all(data_dir).await?;
        };
        if !object_dir.exists() {
            fs_err::tokio::create_dir_all(object_dir).await?;
            fs_err::tokio::create_dir_all(&paths.objects_staging).await?;
        }
        let data_lock = lock::Lock::new(&paths.data_lock)?;
        let objects_lock = lock::Lock::new(&paths.objects_staging_lock)?;

        // Now that we have the locks, we can begin opening files
        let maps = if !paths.index_file.exists() {
            hashes::HashesMap::try_new_with(|k| {
                let name = k.name();
                let dir = paths.hash_base.join(name);
                fs_err::create_dir_all(&dir)?;
                // # Safety
                // The relevant files have been locked for the duration of the Lookup's
                // existence
                unsafe { int_multistore::Lookup::new(dir, name) }
            })?
        } else {
            hashes::HashesMap::try_new_with(|k| {
                let name = k.name();
                let dir = paths.hash_base.join(name);
                // # Safety
                // The relevant files have been locked for the duration of the Lookup's
                // existence
                unsafe { int_multistore::Lookup::open(dir, name) }
            })?
        };

        Ok(Self {
            paths,
            data_lock,
            objects_lock,
            inner: RwLock::new(Inner {
                maps,
                tokens: token::TokenDistributor::new(32).await,
            }),
        })
    }

    pub async fn add(&self, stream: impl AsyncRead) -> anyhow::Result<ObjectId> {
        let token = {
            let read = self.inner.read().await;
            read.tokens.acquire().await
        };

        let to_path = self.paths.objects_staging.join(format!("current-{}", token.id()));
        let mut to_file = fs_err::tokio::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&to_path)
            .await?;
        pin!(stream);
        tokio::io::copy(&mut stream, &mut to_file).await?;
        to_file.flush().await?;
        let to_file = to_file.into_std().await;
        let map = unsafe { Mmap::map(&to_file) }?;

        // We specifically do not want to be holding any form of lock here, as this is the
        // expensive part and want this to be able to run on multiple uploads concurrently.
        let hashes = hashes::Hashes::extract(&map)?;

        {
            let mut write = self.inner.write().await;
            'unfound: {
                let mut candidates = None::<HashSet<_>>;
                for (kind, b) in &hashes {
                    let map = &write.maps[kind];
                    let Some(idx) = map.get_idx(b) else {
                        // `get_idx` returning None means that the hash is unseen, which means that
                        // the file must be new
                        break 'unfound;
                    };
                    let nc = map.get(idx)?.collect::<HashSet<_>>();
                    if let Some(ref mut candidates) = candidates {
                        candidates.retain(|c| nc.contains(c));
                        if candidates.is_empty() {
                            break 'unfound;
                        }
                    } else {
                        candidates = Some(nc);
                    }
                }
                let candidates = candidates.expect("there is at least one hash");

                // If all hashes consistent, check candidate's bytes

                for candidate_id in candidates {
                    let path = self.paths.path_for(ObjectId(candidate_id));
                    // TODO: proper logging
                    let file = fs_err::tokio::File::open(&path).await.context("object was deleted on disk")?;
                    // TODO: Use a custom checker function that compares a `T: Read` and a `&[u8]`
                    let object_map = unsafe { Mmap::map(&file) }?;
                    if map[..] == object_map[..] {
                        // TODO: update metadata
                        // TODO: is there some way to return the ID?
                        drop(map);
                        let _ = fs_err::tokio::remove_file(to_path).await;
                        return Ok(ObjectId(candidate_id));
                    }
                }

                break 'unfound; // Not necessary, but here for clarity
            }
            drop(map);
            drop(to_file);

            let id = write.next_id()?;

            let path = self.paths.path_for(id);
            let dir = path.parent().unwrap();
            if !dir.exists() {
                fs_err::tokio::create_dir_all(dir).await?;
            }
            fs_err::tokio::rename(to_path, path).await?;

            // TODO: Store metadata in a sidecar file
            for (kind, b) in &hashes {
                let map = &mut write.maps[kind];
                if let Some(idx) = map.get_idx(b) {
                    map.insert(idx, b, id.0)?;
                } else {
                    map.set(b, id.0)?;
                }
            }
            drop(token);
            Ok(id)
        }
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        let mut s = self.inner.write().await;
        for (_, map) in &mut s.maps {
            map.flush()?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Inner {
    maps: hashes::HashesMap<int_multistore::Lookup>,
    tokens: token::TokenDistributor,
}

impl Inner {
    fn next_id(&mut self) -> anyhow::Result<ObjectId> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(1);
        let n = N.fetch_add(1, Ordering::SeqCst);
        Ok(ObjectId(NonZeroU64::new(n).unwrap()))
    }
}

#[derive(Debug)]
struct Pather {
    index_file: PathBuf,
    index_write: PathBuf,
    hash_base: PathBuf,
    data_lock: PathBuf,

    objects_staging: PathBuf,
    objects_staging_lock: PathBuf,
    objects_storage: PathBuf,
}

impl Pather {
    fn new(data_dir: &Path, object_dir: &Path) -> Self {
        Self {
            index_file: data_dir.join("index.ark"),
            index_write: data_dir.join(".index.ark~"),
            hash_base: data_dir.to_owned(),
            data_lock: data_dir.join("ARK.LOCK"),

            objects_staging: object_dir.join(".staging"),
            objects_staging_lock: object_dir.join("ARK.LOCK"),
            objects_storage: object_dir.to_owned(),
        }
    }

    fn path_for(&self, id: ObjectId) -> PathBuf {
        let n = id.0.get();
        let last = n & 0xFF;
        let pen = (n >> 8) & 0xFF;

        self.objects_storage.join(format!("{pen:02X}/{last:02X}/{n}"))
    }
}
