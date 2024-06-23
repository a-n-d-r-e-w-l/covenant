use std::{
    fmt::{Debug, Formatter},
    time::{Duration, Instant},
};

use arbitrary::{Arbitrary, Unstructured};
use bstr::BStr;
use log::{debug, info, LevelFilter};
use seqstore::{
    raw_store::checker::{CheckItem, Checker},
    Backing,
};
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

    fn make_backing() -> anyhow::Result<Backing> {
        if false {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open("file.bin")?;
            unsafe { Backing::new_file(f) }.map_err(Into::into)
        } else {
            Backing::new_anon().map_err(Into::into)
        }
    }

    for i in 0..4000 {
        // 0..4000
        if i % 100 == 0 {
            debug!("Beginning {i}");
        }
        let mut rng = rand::rngs::StdRng::seed_from_u64(i);
        let count = rng.gen_range::<usize, _>(50..8000);

        // let mut debug_file = std::io::BufWriter::new(fs_err::File::create("dbg.txt")?);

        let mut checker = Checker::new(make_backing()?)?;
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
                    let l = checker.names().len();
                    if l == 0 {
                        continue;
                    }
                    let name = checker.names().nth(i % l).unwrap();
                    // writeln!(debug_file, "delete {name}")?;
                    with(&mut write_dur, || checker.execute(CheckItem::Remove(name)))?;
                }
            }
        }
        // debug_file.flush()?;
        with(&mut check_dur, || checker.check_all())?;

        let keys = checker.keys().collect::<Vec<_>>();
        with(&mut read_dur, || {
            for key in keys {
                let _ = checker.map().get(key, ToOwned::to_owned).unwrap();
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

    // let mut checker = Checker::new(make_backing()?)?;
    // let mut debug_file = std::io::BufReader::new(std::fs::File::open("dbg.txt")?);
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

    // let mut map = Checker::new(make_backing()?)?;
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
