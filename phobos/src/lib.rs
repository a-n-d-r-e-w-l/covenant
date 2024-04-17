#![deny(private_interfaces)]
#![warn(missing_debug_implementations)]

use binrw::{binrw, BinRead, BinReaderExt, BinResult, BinWrite, Endian};
use camino::Utf8PathBuf;
use fs_err::{File, OpenOptions};
use fst::map::OpBuilder;
use fst::{MapBuilder, Streamer};
use memmap2::Mmap;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

mod indexed_map;
mod multi_indexed;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, BinRead, BinWrite)]
pub struct Id(NonZeroU64);

impl Id {
    pub fn new(from: NonZeroU64) -> Self {
        Self(from)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, BinRead, BinWrite)]
struct InternalId(u64);

#[derive(Debug)]
struct Pather {
    prefix: String,
    base: Utf8PathBuf,
    index: Utf8PathBuf,
    index_write: Utf8PathBuf,
    log: Utf8PathBuf,
    log_backup: Utf8PathBuf,
    iid_map: Utf8PathBuf,
    eid_map: Utf8PathBuf,
}

impl Pather {
    fn new(base: Utf8PathBuf, prefix: String) -> anyhow::Result<Self> {
        // let base = Utf8PathBuf::from_path_buf(std::env::current_dir()?)
        //     .map_err(|_| anyhow!("path contained invalid UTF-8"))?
        //     .join(base);
        Ok(Self {
            index: base.join(format!("{prefix}.idx")),
            index_write: base.join(format!("~{prefix}.idx")),
            log: base.join(format!("{prefix}.log")),
            log_backup: base.join(format!("~{prefix}.log")),
            iid_map: base.join(format!("{prefix}.iid")),
            eid_map: base.join(format!("{prefix}.eid")),

            prefix,
            base,
        })
    }

    fn fst(&self, id: u64, level: u8) -> Utf8PathBuf {
        self.base.join(format!("{}_{id}.{level}.fst", self.prefix))
    }
}

#[derive(Debug)]
pub struct Database {
    paths: Pather,
    index_file: File,
    log_file: File, // TODO: This might need buffering
    count: AtomicU64,
    fst_count: AtomicU64,
    fsts: Vec<LevelFst>,
    held: HashMap<InternalId, Vec<Vec<u8>>>,
    direct_written: usize,

    iid_lookup: indexed_map::IndexedMap,
    eid_lookup: multi_indexed::MultiIndexed,
}

impl Database {
    pub fn new(at: Utf8PathBuf, prefix: String) -> anyhow::Result<Self> {
        let paths = Pather::new(at, prefix.clone())?;
        if paths.base.exists() {
            let mut index_file = OpenOptions::new().read(true).write(true).create(false).open(&paths.index)?;
            let index = Index::read(&mut index_file)?;
            let log_file = OpenOptions::new().read(true).write(true).create(false).open(&paths.log)?;
            // TODO: Restore log
            let fst_count = index.fsts.iter().map(|f| f.id).max().unwrap_or(0);
            let fsts = index
                .fsts
                .into_iter()
                .map(|id| {
                    let fst_file = File::open(paths.fst(id.id, id.level))?;
                    let map = unsafe { Mmap::map(&fst_file) }?;
                    let fst = fst::Map::new(map)?;
                    Ok(LevelFst {
                        count: id.count,
                        id: id.id,
                        level: id.level,
                        fst,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            let mut s = Self {
                index_file,
                log_file,
                count: AtomicU64::new(fsts.iter().map(|f| f.count).sum()),
                fst_count: AtomicU64::new(fst_count),
                fsts,
                held: Default::default(),
                direct_written: 0,
                iid_lookup: indexed_map::IndexedMap::from_file(OpenOptions::new().read(true).write(true).create(false).open(&paths.iid_map)?)?,
                eid_lookup: multi_indexed::MultiIndexed::from_file(OpenOptions::new().read(true).write(true).create(false).open(&paths.eid_map)?)?,

                paths,
            };
            s.restore_log()?;

            Ok(s)
        } else {
            fs_err::create_dir_all(&paths.base)?;
            let index_file = File::create(&paths.index)?;
            let log_file = File::create(&paths.log)?;
            let fsts = vec![];
            let fst_count = 0;

            let mut s = Self {
                index_file,
                log_file,
                count: AtomicU64::new(0),
                fst_count: AtomicU64::new(fst_count),
                fsts,
                held: Default::default(),
                direct_written: 0,
                iid_lookup: indexed_map::IndexedMap::from_file(OpenOptions::new().read(true).write(true).create(true).open(&paths.iid_map)?)?,
                eid_lookup: multi_indexed::MultiIndexed::from_file(OpenOptions::new().read(true).write(true).create(true).open(&paths.eid_map)?)?,

                paths,
            };

            s.write_index()?;

            Ok(s)
        }
    }

    fn restore_log(&mut self) -> anyhow::Result<()> {
        let end = self.log_file.seek(SeekFrom::End(0))?;
        self.log_file.rewind()?;

        fn extract(f: &mut File, end: u64) -> impl Iterator<Item = anyhow::Result<LogItem<'static>>> {
            let mut data = Vec::new();
            let mut e = f.read_to_end(&mut data).err().map(Into::into);
            let err = e.is_some();
            let mut reader = Cursor::new(data);
            std::iter::from_fn(move || {
                if err {
                    return e.take().map(Err);
                }
                match reader.stream_position() {
                    Ok(p) if p < end => {
                        let item = reader.read_be::<LogItem>().map(Some).map_err(Into::into).transpose()?;
                        Some(item)
                    }
                    Ok(_) => None,
                    Err(e) => Some(Err(e.into())),
                }
            })
            .fuse()
        }

        let using_backup = self.paths.log_backup.exists();
        let mut log_backup = if using_backup { Some(File::open(&self.paths.log_backup)?) } else { None };
        let base = log_backup.as_mut().map(|lb| extract(lb, end));
        let items = base.into_iter().flatten().chain(extract(&mut self.log_file, end));

        if !using_backup {
            // Standard restore
            fs_err::copy(&self.paths.log, &self.paths.log_backup)?;
        }
        self.log_file.set_len(0)?;
        self.log_file.rewind()?;

        let mut to_add = HashMap::new();
        // let mut lookup = HashMap::new();

        for item in items {
            match item? {
                LogItem::LookupBackup { iid, off, len } => {
                    self.iid_lookup.set(iid, &(off, len))?;
                }
                LogItem::Insert { key, id } => {
                    to_add.insert(id, key);
                }
                LogItem::TakenIid { iid } => {
                    let old = self.count.fetch_max(iid.0 + 1, Ordering::SeqCst);
                    assert_eq!(old, iid.0);
                }
                LogItem::Flushed => {}
            }
        }

        for (id, bytes) in to_add {
            self.add(bytes.deref(), id)?;
        }
        self.merge()?;

        self.log_file.set_len(0)?;
        self.log_file.rewind()?;

        let _ = fs_err::remove_file(&self.paths.log_backup);
        Ok(())
    }

    fn write_index(&mut self) -> anyhow::Result<()> {
        let count = self.count.load(Ordering::SeqCst);
        // assert_eq!(count, self.fsts.iter().map(|f| f.count).sum::<u64>() + self.held.len() as u64);
        let mut wtr = BufWriter::new(File::create(&self.paths.index_write)?);
        Index {
            total_items: count,
            fsts: self
                .fsts
                .iter()
                .map(|f| IndexFst {
                    id: f.id,
                    level: f.level,
                    count: f.count,
                })
                .collect(),
        }
        .write(&mut wtr)?;
        wtr.flush()?;
        drop(wtr);
        fs_err::rename(&self.paths.index_write, &self.paths.index)?;
        // TODO: write all to second file
        //       flush second file
        //       copy second file bytewise to first file
        //           this preserves `self.index_file` while maintaining durability
        self.index_file = File::open(&self.paths.index)?;

        Ok(())
    }

    const MEM_THRESHOLD: usize = 128;
    const FANOUT: usize = 6;

    fn append_to(&mut self, id: Id, iid: InternalId) -> anyhow::Result<()> {
        let (off, len) = self.iid_lookup.get(iid)?.unwrap();
        self.log(LogItem::LookupBackup { iid, off, len })?;
        let mut items = self.eid_lookup.get(off, len)?;
        // Ok(idx) indicates that `items[idx] == id`
        if let Err(idx) = items.binary_search(&id) {
            items.insert(idx, id);
            let (off, len) = self.eid_lookup.append(&items)?;
            self.iid_lookup.set(iid, &(off, len))?;
        }
        Ok(())
    }

    fn log(&mut self, item: LogItem) -> anyhow::Result<()> {
        log(&mut self.log_file, item)
    }

    pub fn add(&mut self, key: &[u8], id: Id) -> anyhow::Result<()> {
        self.log(LogItem::Insert {
            key: LogCowBytes::Borrowed(key),
            id,
        })?;

        for f in &self.fsts {
            if let Some(iid) = f.fst.get(key) {
                self.direct_written += 1;
                let iid = InternalId(iid);
                self.append_to(id, iid)?;
                if self.direct_written >= Self::MEM_THRESHOLD {
                    self.merge()?;
                }
                return Ok(());
            }
        }
        'merge: {
            // Key is unknown to the FST system, but may be in `held`
            let held = self.filter_held(key);
            if !held.is_empty() {
                for iid in held {
                    self.held.get_mut(&iid).unwrap().push(key.to_owned());
                    self.append_to(id, iid)?;
                }
                break 'merge;
            }

            // Key is unknown to both FST and `held`, so we haven't seen it before
            // Get new IID, create new slot in IID/EID lookups
            let iid = InternalId(self.count.fetch_add(1, Ordering::SeqCst));
            self.log(LogItem::TakenIid { iid })?;
            let (off, len) = self.eid_lookup.append(&[id])?;
            self.iid_lookup.set(iid, &(off, len))?;
            self.held.insert(iid, vec![key.to_owned()]);
        }

        if self.held.values().map(|v| v.len()).sum::<usize>() >= Self::MEM_THRESHOLD {
            self.merge()?;
        }

        Ok(())
    }

    fn filter_held(&self, key: &[u8]) -> Vec<InternalId> {
        let mut held = self
            .held
            .iter()
            .flat_map(|(iid, v)| v.iter().map(|d| (*iid, d)))
            .filter(|(_, d)| &d[..] == key)
            .map(|(iid, _)| iid)
            .collect::<Vec<_>>();
        held.sort_unstable();
        held.dedup();
        held
    }

    pub fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Vec<Id>>> {
        let held = self.filter_held(key);

        for f in &self.fsts {
            if let Some(iid) = f.fst.get(key) {
                let iid = InternalId(iid);
                let (off, len) = self.iid_lookup.get(iid)?.unwrap();
                let items = self.eid_lookup.get(off, len)?;
                assert!(held.is_empty()); // *Should* be guaranteed by the checks in `add(..)`
                return Ok(Some(items));
            }
        }

        if held.is_empty() {
            return Ok(None);
        }

        let mut items = held
            .into_iter()
            .map(|iid| {
                let (off, len) = self.iid_lookup.get(iid)?.unwrap();
                let items = self.eid_lookup.get(off, len)?;
                Ok(items)
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        items.sort_unstable();
        items.dedup();
        Ok(Some(items))
    }

    fn merge_fsts(&mut self, filter: impl Fn(&LevelFst) -> bool, level: u8, force_merge: bool) -> anyhow::Result<()> {
        let mut items = self
            .held
            .drain()
            .flat_map(|(id, v)| v.into_iter().map(move |d| (id, d)))
            .collect::<Vec<_>>();
        items.sort_by(|(_, a), (_, b)| a.cmp(b).reverse());

        let to_merge = self.fsts.iter().filter(|f| filter(f)).collect::<Vec<_>>();
        if to_merge.is_empty() && items.is_empty() && !force_merge {
            return Ok(());
        }

        let target_level = if to_merge.is_empty() { level } else { level + 1 };

        let new_id = self.fst_count.fetch_add(1, Ordering::SeqCst) + 1;
        let target = self.paths.fst(new_id, target_level);
        // Build new FST
        let file = OpenOptions::new().create(true).write(true).read(true).open(&target)?;
        let mut wtr = BufWriter::new(file);

        let mut builder = MapBuilder::new(&mut wtr)?;
        let mut stream = OpBuilder::new();
        for merge in &to_merge {
            stream = stream.add(&merge.fst);
        }
        let mut stream = stream.union();

        let mut count = 0;
        let mut previous: Option<Vec<u8>> = None;
        let mut add = |key: &[u8], value| {
            if previous.as_ref().is_some_and(|p| p == key) {
                return Ok(());
            }
            count += 1;
            previous = Some(key.to_owned());
            builder.insert(key, value)
        };

        while let Some((key, streamed)) = stream.next() {
            if streamed.len() > 1 {
                let first = streamed[0].value;
                assert!(streamed.iter().all(|v| v.value == first), "{streamed:?}");
            }
            while items.last().is_some_and(|(_, d)| &d[..] < key) {
                let (id, d) = items.pop().unwrap();
                add(&d, id.0)?;
            }

            add(key, streamed[0].value)?;
        }

        while let Some((id, d)) = items.pop() {
            add(&d, id.0)?;
        }
        builder.finish()?;
        wtr.flush()?;
        drop(stream);

        let to_remove = to_merge.iter().map(|f| (f.id, f.level)).collect::<Vec<_>>();

        for (merged_id, merged_level) in to_remove {
            let origin = self.paths.fst(merged_id, merged_level);
            if origin.exists() {
                fs_err::remove_file(&origin)?;
            } else {
                println!("cannot remove {origin}"); // TODO: Proper logging
            }
        }

        let merged = to_merge.iter().map(|r| r.id).collect::<HashSet<_>>();
        self.fsts.retain(|it| !merged.contains(&it.id));

        if count == 0 {
            drop(wtr);
            fs_err::remove_file(target)?;
        } else {
            let file = wtr.into_inner()?;
            let mmap = unsafe { Mmap::map(&file) }?;
            let new = LevelFst {
                count,
                id: new_id,
                level: target_level,
                fst: fst::Map::new(mmap)?,
            };

            self.fsts.push(new);
        }
        self.write_index()?;

        self.log(LogItem::Flushed)?;
        self.log_file.set_len(0)?;
        self.log_file.rewind()?;
        self.direct_written = 0;
        Ok(())
    }

    pub fn merge(&mut self) -> anyhow::Result<()> {
        let force_merge = self.direct_written >= Self::MEM_THRESHOLD;
        if self.held.is_empty() && !force_merge {
            return Ok(());
        }

        let mut levels = self
            .fsts
            .iter()
            .map(|f| f.level)
            .fold(HashMap::new(), |mut m, l| {
                *m.entry(l).or_insert(0) += 1_usize;
                m
            })
            .into_iter()
            .collect::<Vec<_>>();
        levels.sort_by(|(a, _), (b, _)| a.cmp(b).reverse());

        let mut maximum_level = 0;
        while let Some((level, count)) = levels.pop() {
            if count < Self::FANOUT {
                break;
            }
            maximum_level = level;
        }

        self.merge_fsts(|f| f.level <= maximum_level, maximum_level, force_merge)?;

        Ok(())
    }

    pub fn unify_fsts(&mut self) -> anyhow::Result<()> {
        let total = self.held.len() + self.fsts.iter().map(|f| f.count as usize).sum::<usize>();
        // No, this isn't exact, but it's good enough
        let level = (total * Self::FANOUT / 2 / Self::MEM_THRESHOLD).ilog(Self::FANOUT);
        let level = level.clamp(0, u8::MAX as _) as u8;
        self.fst_count.store(0, Ordering::SeqCst);
        self.merge_fsts(|_| true, level, true)
    }
}

fn log(log_file: &mut File, item: LogItem) -> anyhow::Result<()> {
    let mut buffer = Vec::new();
    item.write_be(&mut Cursor::new(&mut buffer)).unwrap();
    log_file.write_all(&buffer)?;
    log_file.flush()?;
    Ok(())
}

#[binrw]
#[derive(Debug)]
enum LogItem<'a> {
    #[brw(magic = 0_u8)]
    LookupBackup { iid: InternalId, off: u64, len: u64 },
    #[brw(magic = 1_u8)]
    Insert { key: LogCowBytes<'a>, id: Id },
    #[brw(magic = 2_u8)]
    TakenIid { iid: InternalId },
    #[brw(magic = 3_u8)]
    Flushed,
}

#[derive(Debug)]
enum LogCowBytes<'a> {
    Owned(Vec<u8>),
    Borrowed(&'a [u8]),
}

impl<'a> Deref for LogCowBytes<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(b) => &b[..],
            Self::Borrowed(b) => b,
        }
    }
}

impl<'a> BinWrite for LogCowBytes<'a> {
    type Args<'b> = <[u8] as BinWrite>::Args<'b>;

    fn write_options<W: Write + Seek>(&self, writer: &mut W, endian: Endian, args: Self::Args<'_>) -> BinResult<()> {
        let len = match self {
            Self::Owned(b) => b.len(),
            Self::Borrowed(b) => b.len(),
        };
        <u64 as BinWrite>::write_options(&len.try_into().unwrap(), writer, endian, ())?;
        <[u8] as BinWrite>::write_options(self, writer, endian, args)
    }
}

impl<'a> BinRead for LogCowBytes<'a> {
    type Args<'b> = ();

    fn read_options<R: Read + Seek>(reader: &mut R, endian: Endian, _: Self::Args<'_>) -> BinResult<Self> {
        let len = <u64 as BinRead>::read_options(reader, endian, ())?;
        let b = <Vec<u8> as BinRead>::read_options(
            reader,
            endian,
            binrw::VecArgs::builder().count(len.try_into().unwrap()).finalize(),
        )?;
        Ok(Self::Owned(b))
    }
}

struct LevelFst {
    count: u64,
    id: u64,
    level: u8,
    fst: fst::Map<Mmap>,
}

impl Debug for LevelFst {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("count", &self.count)
            .field("id", &self.id)
            .field("level", &self.level)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, BinRead, BinWrite)]
struct IndexFst {
    id: u64,
    level: u8,
    count: u64,
}

#[binrw]
#[derive(Debug)]
#[brw(big, magic = b"\xFEDEIindx")]
struct Index {
    total_items: u64,

    #[br(temp)]
    #[bw(calc = fsts.len() as u64)]
    len: u64,
    #[br(count = len)]
    fsts: Vec<IndexFst>,
}
