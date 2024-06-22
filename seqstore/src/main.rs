use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    hash::Hash,
    io::BufRead,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use arbitrary::{Arbitrary, Unstructured};
use bstr::{BStr, ByteSlice};
use indexmap::IndexMap;
use log::{debug, info, trace, LevelFilter};
use seqstore::FileMap;
use rand::{Rng, SeedableRng};

fn main() -> anyhow::Result<()> {
    simplelog::TermLogger::init(
        LevelFilter::Trace,
        simplelog::ConfigBuilder::new().add_filter_allow_str("seqstore").build(),
        Default::default(),
        Default::default(),
    )?;

    let mut total = 0;
    let mut read_dur = Duration::from_secs(0);
    let mut write_dur = Duration::from_secs(0);
    let mut check_dur = Duration::from_secs(0);
    let mut reopen_dur = Duration::from_secs(0);
    let mut total_bytes = 0;

    for i in 0..4000 {
        // 0..4000
        if i % 100 == 0 {
            debug!("Beginning {i}");
        }
        let mut rng = rand::rngs::StdRng::seed_from_u64(i);
        let count = rng.gen_range::<usize, _>(50..8000);

        // let mut debug_file = std::io::BufWriter::new(fs_err::File::create("dbg.txt")?);

        let mut checker = Checker::new()?;
        for j in 0..count {
            let mut bytes = vec![0_u8; rng.gen_range(2..=0b111_11111111)];
            total_bytes += bytes.len();
            rng.fill(&mut bytes[..]);

            let ac = Action::arbitrary(&mut Unstructured::new(&bytes[..])).unwrap();

            match ac {
                Action::Add(b) => {
                    const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
                    let c = ALPHA[j % ALPHA.len()];
                    let b = &vec![c; b.len()];
                    // writeln!(debug_file, "write {:?} {j} -> {}", char::from(c), b.len())?;
                    with(&mut write_dur, || checker.execute(CheckItem::Add(j, b)))?;
                }
                Action::Remove(i) => {
                    let l = checker.names.len();
                    if l == 0 {
                        continue;
                    }
                    let name = checker.names.keys().copied().nth(i % l).unwrap();
                    // writeln!(debug_file, "delete {name}")?;
                    with(&mut write_dur, || checker.execute(CheckItem::Remove(name)))?;
                }
            }
        }
        // debug_file.flush()?;
        with(&mut check_dur, || checker.check_all())?;

        let keys = checker.check.keys().copied().collect::<Vec<_>>();
        with(&mut read_dur, || {
            for key in keys {
                let _ = checker.map.get(key).unwrap();
            }
        });
        // seqstore::debug_map(&checker.map)?;
        with(&mut reopen_dur, || checker.reopen())?;
        checker.check_all()?;
        total += count;
    }
    info!("{total} total items");
    info!("{total_bytes} total bytes");
    info!("Write: {:?}", write_dur);
    info!("Read : {:?}", read_dur);
    info!("Check: {:?}", check_dur);
    info!("Open : {:?}", reopen_dur);
    return Ok(());

    // let mut checker = Checker::new()?;
    // let mut debug_file = std::io::BufReader::new(fs_err::File::open("dbg.txt")?);
    // for line in debug_file.lines() {
    //     let line = line?;
    //     match &line.split(' ').collect::<Vec<_>>()[..] {
    //         ["write", name, "->", len] => {
    //             let name = name.parse::<usize>()?;
    //             let len = len.parse::<usize>()?;
    //             let b = vec![(name & 0b11111) as u8 | 0b111_00000; len];
    //             checker.execute(CheckItem::Add(name, &b[..]))?;
    //         }
    //         ["delete", name] => {
    //             let name = name.parse::<usize>()?;
    //             checker.execute(CheckItem::Remove(name))?;
    //         }
    //         _ => panic!("{line:?}"),
    //     }
    // }
    // checker.check_all()?;
    // return Ok(());

    // let mut map = Checker::new()?;
    // for item in [
    //     CheckItem::Add("a", &[b'a'; 20]),
    //     CheckItem::Add("b", &[b'b'; 20]),
    //     CheckItem::Remove("a"),
    //     CheckItem::Remove("b"),
    //     CheckItem::Add("c", &[b'c'; 30]),
    //     CheckItem::Add("d", &[b'd'; 5]),
    //     CheckItem::Add("e", &[b'e'; 5]),
    //     // CheckItem::Print,
    //     CheckItem::CheckAll,
    // ] {
    //     println!("Processing {item:?}");
    //     map.execute(item)?;
    // }
    // map.reopen()?;
    // map.check_all()?;
    // Ok(())
}

fn with<R>(d: &mut Duration, f: impl FnOnce() -> R) -> R {
    let start = Instant::now();
    let r = f();
    let dur = start.elapsed();
    *d += dur;
    r
}

enum Action<'a> {
    Add(&'a [u8]),
    Remove(usize),
}

impl<'a> Arbitrary<'a> for Action<'a> {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let tag = u.arbitrary::<u8>()?;
        if tag <= 100 {
            Ok(Self::Remove(u.arbitrary()?))
        } else {
            Ok(Self::Add(u.arbitrary()?))
        }
    }
}

impl Debug for Action<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add(bytes) => f.debug_tuple("Add").field(&BStr::new(bytes)).finish(),
            Self::Remove(idx) => f.debug_tuple("Remove").field(idx).finish(),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum CheckItem<'a, N> {
    Add(N, &'a [u8]),
    Remove(N),
    Check(N),
    CheckAll,
    Debug,
    Print,
}

impl<N: Debug> Debug for CheckItem<'_, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add(name, bytes) => f.debug_tuple("Add").field(name).field(&BStr::new(bytes)).finish(),
            Self::Remove(name) => f.debug_tuple("Remove").field(name).finish(),
            Self::Check(name) => f.debug_tuple("Check").field(name).finish(),
            Self::CheckAll => f.debug_tuple("CheckAll").finish(),
            Self::Debug => f.debug_tuple("Debug").finish(),
            Self::Print => f.debug_tuple("Print").finish(),
        }
    }
}

#[derive(Debug)]
struct Checker<N> {
    check: IndexMap<u64, Vec<u8>>,
    names: IndexMap<N, u64>,
    map: FileMap,
}

impl<N: Hash + Eq + Debug + Copy> Checker<N> {
    fn new() -> anyhow::Result<Self> {
        let b = if false {
            Some(
                fs_err::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open("file.bin")?,
            )
        } else {
            None
        };
        Ok(Self {
            check: IndexMap::new(),
            names: IndexMap::new(),
            map: FileMap::new(b)?,
        })
    }

    fn execute(&mut self, item: CheckItem<N>) -> anyhow::Result<()> {
        match item {
            CheckItem::Add(name, bytes) => {
                let at = self.map.add(bytes)?;
                assert!(self.names.insert(name, at).is_none());
                assert!(self.check.insert(at, bytes.to_vec()).is_none());
                // debug!("{name:?} stored at {at}");
                Ok(())
            }
            CheckItem::Remove(name) => {
                // debug!("removing {name:?}");
                let at = self.names.swap_remove(&name).expect("removing name that was never inserted");
                let check = self.check.swap_remove(&at).expect("removing location that was never added");
                let stored = self.map.remove(at).context("could not get item")?;
                if check != stored {
                    Err(anyhow!(
                        "mismatch: expected {:?}, found {:?}",
                        BStr::new(&check),
                        BStr::new(&stored)
                    ))
                } else {
                    Ok(())
                }
            }
            CheckItem::Check(name) => {
                let at = *self.names.get(&name).expect("checking name that was never inserted");
                let check = self.check.get(&at).expect("checking location that was never added");
                let stored = self.map.get(at).context("could not get item")?;
                if *check != stored {
                    Err(anyhow!(
                        "mismatch: expected {:?}, found {:?}",
                        BStr::new(check),
                        BStr::new(&stored)
                    ))
                } else {
                    Ok(())
                }
            }
            CheckItem::CheckAll => self.check_all(),
            CheckItem::Debug => seqstore::debug_map(&self.map),
            CheckItem::Print => {
                let b = &self.map.backing[..];
                trace!("{:?}", BStr::new(b.trim_end_with(|b| b == '\0')));
                Ok(())
            }
        }
    }

    fn reopen(&mut self) -> anyhow::Result<()> {
        let map = std::mem::replace(&mut self.map, FileMap::new(None).unwrap());
        let backing = map.close()?;
        let map = FileMap::open(backing)?;
        self.map = map;
        Ok(())
    }

    fn check_all(&mut self) -> anyhow::Result<()> {
        for name in self.names.keys().copied().collect::<Vec<_>>() {
            self.execute(CheckItem::Check(name))?;
        }
        Ok(())
    }
}
