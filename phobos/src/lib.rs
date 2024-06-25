#![deny(private_interfaces)]
#![warn(missing_debug_implementations)]

use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
    io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use anyhow::anyhow;
use bytes::Bytes;
use fs_err::{File, OpenOptions};
use fst::{map::OpBuilder, MapBuilder, Streamer};
use memmap2::Mmap;
use varuint::{ReadVarint, WriteVarint};

#[derive(Debug)]
pub struct DatabaseOptions {
    at: PathBuf,
    prefix: String,
    unify_on_open: bool,
    fanout: usize,
    memory_threshold: usize,
    create: bool,
}

impl DatabaseOptions {
    pub fn new(at: PathBuf, prefix: String) -> Self {
        Self {
            at,
            prefix,
            unify_on_open: false,
            fanout: 6,
            memory_threshold: 128,
            create: true,
        }
    }

    /// # Safety
    ///
    /// From calling this function to closing the returned Database, the relevant files within
    /// the provided directory **must not** be modified, whether in- or out-of-process. All relevant
    /// files start with the provided prefix.
    ///
    /// Modifying any such file will likely result in a panic, but may result in incorrect results
    /// being returned instead. The `fst` crate guarantees that modifying the underlying files will
    /// not cause memory safety.
    pub unsafe fn open(self) -> anyhow::Result<Database> {
        let paths = Pather::new(self.at, self.prefix.clone())?;
        let mut s = if paths.index.exists() {
            let mut index_file = OpenOptions::new().read(true).write(true).create(false).open(&paths.index)?;
            let index = Index::read(&mut index_file)?;
            let log_file = OpenOptions::new().read(true).write(true).create(false).open(&paths.log)?;
            let fst_count = index.fsts.iter().map(|f| f.id as usize).max().unwrap_or(0);
            let fsts = index
                .fsts
                .into_iter()
                .map(|fs| {
                    let fst_file = File::open(paths.fst(fs.id, fs.level))?;
                    let map = unsafe { Mmap::map(&fst_file) }?;
                    let fst = fst::Map::new(map)?;
                    Ok(LevelFst {
                        count: fs.count,
                        id: fs.id,
                        level: fs.level,
                        fst,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            let mut s = Database {
                index_file,
                log_file,
                count: fsts.iter().map(|f| f.count as usize).sum(),
                fst_count,
                fsts,
                held: Default::default(),
                paths,
                fanout: self.fanout,
                memory_threshold: self.memory_threshold,
            };
            s.restore_log()?;

            s
        } else {
            if !self.create {
                return Err(anyhow!("directory does not exist"));
            }
            fs_err::create_dir_all(&paths.base)?;
            let index_file = File::create(&paths.index)?;
            let log_file = File::create(&paths.log)?;
            let fsts = vec![];

            let mut s = Database {
                index_file,
                log_file,
                count: 0,
                fst_count: 0,
                fsts,
                held: Default::default(),
                paths,
                fanout: self.fanout,
                memory_threshold: self.memory_threshold,
            };

            s.write_index()?;

            s
        };

        if self.unify_on_open {
            s.unify_fsts()?;
        }

        Ok(s)
    }

    pub fn unify(self, unify: bool) -> Self {
        Self {
            unify_on_open: unify,
            ..self
        }
    }

    pub fn create(self, create: bool) -> Self {
        Self { create, ..self }
    }

    pub fn fanout(self, fanout: usize) -> Self {
        Self {
            fanout: fanout.max(2),
            ..self
        }
    }

    pub fn write_threshold(self, threshold: usize) -> Self {
        Self {
            memory_threshold: threshold.max(16),
            ..self
        }
    }
}

#[derive(Debug)]
pub struct Database {
    paths: Pather,
    index_file: File,
    log_file: File, // This cannot be a BufWriter, as we also need to read from it
    count: usize,
    fst_count: usize,
    fsts: Vec<LevelFst>,
    held: HashMap<Bytes, u64>,
    fanout: usize,
    memory_threshold: usize,
}

impl Database {
    pub fn options(at: PathBuf, prefix: String) -> DatabaseOptions {
        DatabaseOptions::new(at, prefix)
    }

    fn restore_log(&mut self) -> anyhow::Result<()> {
        let end = self.log_file.seek(SeekFrom::End(0))?;
        self.log_file.rewind()?;

        fn extract(f: &mut impl Read, end: u64) -> impl Iterator<Item = anyhow::Result<LogItem>> {
            let mut data = Vec::new();
            let mut e = f.read_to_end(&mut data).err().map(Into::into);
            let err = e.is_some();
            let mut reader = Cursor::new(data);
            std::iter::from_fn(move || {
                if err {
                    return e.take().map(Err);
                }
                match reader.stream_position() {
                    Ok(p) if p < end => LogItem::read(&mut reader).map(Some).map_err(Into::into).transpose(),
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

        for item in items {
            match item? {
                LogItem::Insert { key, value } => {
                    to_add.insert(key, value);
                }
                LogItem::Flushed => {}
            }
        }

        for (key, value) in to_add {
            self.set(key, value)?;
        }
        self.merge()?;

        self.log_file.set_len(0)?;
        self.log_file.rewind()?;

        let _ = fs_err::remove_file(&self.paths.log_backup);
        Ok(())
    }

    fn write_index(&mut self) -> anyhow::Result<()> {
        let mut wtr = BufWriter::new(File::create(&self.paths.index_write)?);
        Index {
            fsts: self
                .fsts
                .iter()
                .map(|fs| IndexFst {
                    id: fs.id,
                    level: fs.level,
                    count: fs.count,
                })
                .collect(),
        }
        .write(&mut wtr)?;
        wtr.flush()?;
        drop(wtr);
        fs_err::rename(&self.paths.index_write, &self.paths.index)?;
        self.index_file = File::open(&self.paths.index)?;

        Ok(())
    }

    fn log(&mut self, item: LogItem) -> anyhow::Result<()> {
        item.write(&mut self.log_file)?;
        self.log_file.flush()?;
        Ok(())
    }

    pub fn set(&mut self, key: Bytes, value: u64) -> anyhow::Result<()> {
        self.log(LogItem::Insert { key: key.clone(), value })?;

        if self.held.insert(key, value).is_none() {
            self.count += 1;
        }

        if self.held.len() >= self.memory_threshold {
            self.merge()?;
        }

        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Option<u64> {
        if let Some(id) = self.held.get(key) {
            return Some(*id);
        }

        let mut found = vec![];
        for f in &self.fsts {
            if let Some(iid) = f.fst.get(key) {
                found.push((f, iid));
            }
        }

        found.into_iter().max_by_key(|(f, _)| f.id).map(|(_, v)| v)
    }

    fn merge_fsts(&mut self, filter: impl Fn(&LevelFst) -> bool) -> anyhow::Result<()> {
        let mut items = self.held.drain().collect::<Vec<_>>();
        items.sort_by(|(a, _), (b, _)| a.cmp(b).reverse());

        let to_merge = self.fsts.iter().filter(|f| filter(f)).collect::<Vec<_>>();
        if to_merge.is_empty() && items.is_empty() {
            return Ok(());
        }
        let target_level = if to_merge.is_empty() {
            self.calculate_level(items.len())
        } else {
            self.calculate_level(items.len() + to_merge.iter().map(|fs| fs.count as usize).sum::<usize>())
        };

        let new_id = self.fst_count as u64;
        self.fst_count += 1;

        // Build new FST
        let file = OpenOptions::new().create(true).write(true).read(true).open(&self.paths.write_fst)?;
        let mut wtr = BufWriter::new(file);

        let mut builder = MapBuilder::new(&mut wtr)?;
        let mut stream = OpBuilder::new();
        for merge in &to_merge {
            stream = stream.add(&merge.fst);
        }
        let mut stream = stream.union();

        let mut count = 0;
        let mut previous: Option<Bytes> = None;
        let mut add = |key: Bytes, value| {
            if previous.as_ref().is_some_and(|p| *p == key) {
                return Ok(());
            }
            count += 1;
            previous = Some(key.clone());
            builder.insert(key, value)
        };

        while let Some((key, idxs)) = stream.next() {
            let max = idxs.iter().max_by_key(|id| to_merge[id.index].id).expect("non-empty");
            while items.last().is_some_and(|(d, _)| d < key) {
                let (id, d) = items.pop().unwrap();
                add(id, d)?;
            }
            add(Bytes::from(key.to_owned()), max.value)?;
        }

        while let Some((id, d)) = items.pop() {
            add(id, d)?;
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
                println!("cannot remove {}", origin.display()); // TODO: Proper logging
            }
        }

        let merged = to_merge.iter().map(|r| r.id).collect::<HashSet<_>>();
        self.fsts.retain(|it| !merged.contains(&it.id));

        if count == 0 {
            drop(wtr);
            fs_err::remove_file(&self.paths.write_fst)?;
        } else {
            drop(wtr);
            let target = self.paths.fst(new_id, self.calculate_level(count as usize));
            fs_err::rename(&self.paths.write_fst, &target)?;
            let file = File::open(&target)?;
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

        Ok(())
    }

    pub fn merge(&mut self) -> anyhow::Result<()> {
        if self.held.is_empty() {
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

        let mut maximum_level = None;
        while let Some((level, count)) = levels.pop() {
            if count < self.fanout {
                break;
            }
            maximum_level = Some(level);
        }

        if let Some(max) = maximum_level {
            self.merge_fsts(|f| f.level <= max)?;
        } else {
            self.merge_fsts(|_| false)?;
        }

        Ok(())
    }

    pub fn unify_fsts(&mut self) -> anyhow::Result<()> {
        self.fst_count = 0;
        self.merge_fsts(|_| true)
    }

    fn calculate_level(&self, count: usize) -> u8 {
        // count_(n+1) = count_n * Self::FANOUT, count_0 = Self::MEM_THRESHOLD
        // => count_n = Self::MEM_THRESHOLD * Self::FANOUT^(n)
        // => n = log_(Self::FANOUT)(n / Self::MEM_THRESHOLD) clamped to appropriate ranges
        (count / self.memory_threshold).max(1).ilog(self.fanout).clamp(0, u8::MAX as _) as u8
    }
}

#[derive(Debug)]
struct Pather {
    prefix: String,
    base: PathBuf,
    index: PathBuf,
    index_write: PathBuf,
    write_fst: PathBuf,
    log: PathBuf,
    log_backup: PathBuf,
}

impl Pather {
    fn new(base: PathBuf, prefix: String) -> anyhow::Result<Self> {
        Ok(Self {
            index: base.join(format!("{prefix}.idx")),
            index_write: base.join(format!("~{prefix}.idx")),
            log: base.join(format!("{prefix}.log")),
            log_backup: base.join(format!("~{prefix}.log")),
            write_fst: base.join(format!("~{prefix}._.fst")),

            prefix,
            base,
        })
    }

    fn fst(&self, id: u64, level: u8) -> PathBuf {
        self.base.join(format!("{}_{id}.{level}.fst", self.prefix))
    }
}

#[derive(Debug)]
enum LogItem {
    Insert { key: Bytes, value: u64 },
    Flushed,
}

impl LogItem {
    fn write(&self, w: &mut impl Write) -> std::io::Result<()> {
        match self {
            LogItem::Insert { key, value } => {
                w.write_all(&[0])?;
                w.write_varint(key.len() as u64)?;
                w.write_all(key)?;
                w.write_varint(*value)?;
                Ok(())
            }
            LogItem::Flushed => {
                w.write_all(&[1])?;
                Ok(())
            }
        }
    }

    fn read(mut r: impl Read) -> std::io::Result<Self> {
        let mut first = [0];
        r.read_exact(&mut first)?;
        match first[0] {
            0 => {
                let len = <_ as ReadVarint<u64>>::read_varint(&mut r)? as usize;
                let mut buf = vec![0; len];
                r.read_exact(&mut buf)?;
                let value = <_ as ReadVarint<u64>>::read_varint(&mut r)?;
                Ok(Self::Insert {
                    key: Bytes::from(buf),
                    value,
                })
            }
            1 => Ok(Self::Flushed),
            _ => Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
        }
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

#[derive(Debug)]
struct IndexFst {
    id: u64,
    level: u8,
    count: u64,
}

#[derive(Debug)]
struct Index {
    fsts: Vec<IndexFst>,
}

impl Index {
    const MAGIC: &'static [u8] = b"\xFEruFSTg\xAA";

    fn write(&self, w: &mut impl Write) -> std::io::Result<()> {
        w.write_all(Self::MAGIC)?;

        w.write_varint(self.fsts.len() as u64)?;
        for &IndexFst { id, level, count } in &self.fsts {
            w.write_varint(id)?;
            w.write_all(&[level])?;
            w.write_varint(count)?;
        }

        Ok(())
    }

    fn read(r: &mut impl Read) -> std::io::Result<Self> {
        let mut buf = [0; Self::MAGIC.len()];
        r.read_exact(&mut buf)?;
        if buf != Self::MAGIC {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
        }

        let len = <_ as ReadVarint<u64>>::read_varint(r)? as usize;
        let mut fsts = Vec::with_capacity(len);
        for _ in 0..len {
            let id = r.read_varint()?;
            let mut buf = [0];
            r.read_exact(&mut buf)?;
            let level = buf[0];
            let count = r.read_varint()?;
            fsts.push(IndexFst { id, level, count })
        }

        Ok(Self { fsts })
    }
}
