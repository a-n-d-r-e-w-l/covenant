#![deny(private_interfaces)]
#![warn(missing_debug_implementations)]

use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
    io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use binrw::{binrw, BinRead, BinReaderExt, BinResult, BinWrite, Endian};
use bytes::Bytes;
use fs_err::{File, OpenOptions};
use fst::{map::OpBuilder, MapBuilder, Streamer};
use memmap2::Mmap;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, BinRead, BinWrite)]
struct InternalId(u64);

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
pub struct Database {
    paths: Pather,
    index_file: File,
    log_file: File, // TODO: This might need buffering
    count: usize,
    fst_count: usize,
    fsts: Vec<LevelFst>,
    held: HashMap<Bytes, u64>,
}

impl Database {
    pub fn new(at: PathBuf, prefix: String) -> anyhow::Result<Self> {
        let paths = Pather::new(at, prefix.clone())?;
        if paths.base.exists() {
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

            let mut s = Self {
                index_file,
                log_file,
                count: fsts.iter().map(|f| f.count as usize).sum(),
                fst_count,
                fsts,
                held: Default::default(),
                paths,
            };
            s.restore_log()?;

            Ok(s)
        } else {
            fs_err::create_dir_all(&paths.base)?;
            let index_file = File::create(&paths.index)?;
            let log_file = File::create(&paths.log)?;
            let fsts = vec![];

            let mut s = Self {
                index_file,
                log_file,
                count: 0,
                fst_count: 0,
                fsts,
                held: Default::default(),

                paths,
            };

            s.write_index()?;

            Ok(s)
        }
    }

    fn restore_log(&mut self) -> anyhow::Result<()> {
        let end = self.log_file.seek(SeekFrom::End(0))?;
        self.log_file.rewind()?;

        fn extract(f: &mut File, end: u64) -> impl Iterator<Item = anyhow::Result<LogItem>> {
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

        for item in items {
            match item? {
                LogItem::Insert { key, value } => {
                    to_add.insert(key.0, value);
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
            total_items: self.count as u64,
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

    const MEM_THRESHOLD: usize = 128;
    const FANOUT: usize = 6;

    fn log(&mut self, item: LogItem) -> anyhow::Result<()> {
        log(&mut self.log_file, item)
    }

    pub fn set(&mut self, key: Bytes, value: u64) -> anyhow::Result<()> {
        self.log(LogItem::Insert {
            key: LogBytes(key.clone()),
            value,
        })?;

        if self.held.insert(key, value).is_none() {
            self.count += 1;
        }

        if self.held.len() >= Self::MEM_THRESHOLD {
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
            Self::calculate_level(items.len())
        } else {
            Self::calculate_level(items.len() + to_merge.iter().map(|fs| fs.count as usize).sum::<usize>())
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
            let target = self.paths.fst(new_id, Self::calculate_level(count as usize));
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
            if count < Self::FANOUT {
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

    fn calculate_level(count: usize) -> u8 {
        // count_(n+1) = count_n * Self::FANOUT, count_0 = Self::MEM_THRESHOLD
        // => count_n = Self::MEM_THRESHOLD * Self::FANOUT^(n)
        // => n = log_(Self::FANOUT)(n / Self::MEM_THRESHOLD) clamped to appropriate ranges
        (count / Self::MEM_THRESHOLD).max(1).ilog(Self::FANOUT).clamp(0, u8::MAX as _) as u8
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
enum LogItem {
    #[brw(magic = 0_u8)]
    Insert { key: LogBytes, value: u64 },
    #[brw(magic = 1_u8)]
    Flushed,
}

#[derive(Debug)]
struct LogBytes(Bytes);

impl BinWrite for LogBytes {
    type Args<'b> = <[u8] as BinWrite>::Args<'b>;

    fn write_options<W: Write + Seek>(&self, writer: &mut W, endian: Endian, args: Self::Args<'_>) -> BinResult<()> {
        <u64 as BinWrite>::write_options(&self.0.len().try_into().unwrap(), writer, endian, ())?;
        <[u8] as BinWrite>::write_options(&self.0, writer, endian, args)
    }
}

impl BinRead for LogBytes {
    type Args<'b> = ();

    fn read_options<R: Read + Seek>(reader: &mut R, endian: Endian, _: Self::Args<'_>) -> BinResult<Self> {
        let len = <u64 as BinRead>::read_options(reader, endian, ())?;
        let b = <Vec<u8> as BinRead>::read_options(
            reader,
            endian,
            binrw::VecArgs::builder().count(len.try_into().unwrap()).finalize(),
        )?;
        Ok(Self(Bytes::from(b)))
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
