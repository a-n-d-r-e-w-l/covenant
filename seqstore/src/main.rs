use std::collections::HashMap;

use anyhow::{anyhow, Context};
use bstr::{BStr, ByteSlice};
use seqstore::FileMap;

fn main() -> anyhow::Result<()> {
    let mut map = Checker::new()?;
    for item in [
        CheckItem::Add("a", BStr::new(b"abc")),
        CheckItem::Add("b", BStr::new(b"bbcdeawdadadasddldjkmeb")),
        CheckItem::Add("c", BStr::new(b"abcdefg")),
        CheckItem::Add("d", BStr::new(b"abcdefgh")),
        CheckItem::CheckAll,
        CheckItem::Remove("b"),
        CheckItem::CheckAll,
        CheckItem::Debug,
        CheckItem::Print,
        CheckItem::Add("e", BStr::new(b"qqqfp")),
        CheckItem::Add("j", BStr::new(b"4")),
        CheckItem::Print,
        CheckItem::CheckAll,
    ] {
        println!("Processing {item:?}");
        map.execute(item)?;
    }
    Ok(())
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum CheckItem {
    Add(&'static str, &'static BStr),
    Remove(&'static str),
    Check(&'static str),
    CheckAll,
    Debug,
    Print,
}

#[derive(Debug)]
struct Checker {
    check: HashMap<u64, &'static [u8]>,
    names: HashMap<&'static str, u64>,
    map: FileMap,
}

impl Checker {
    fn new() -> anyhow::Result<Self> {
        let f = fs_err::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("file.bin")?;
        Ok(Self {
            check: HashMap::new(),
            names: HashMap::new(),
            map: FileMap::new(Some(f))?,
        })
    }

    fn execute(&mut self, item: CheckItem) -> anyhow::Result<()> {
        match item {
            CheckItem::Add(name, bytes) => {
                let at = self.map.add(bytes)?;
                assert!(self.names.insert(name, at).is_none());
                assert!(self.check.insert(at, bytes).is_none());
                println!("{name:?} stored at {at}");
                Ok(())
            }
            CheckItem::Remove(name) => {
                let at = self.names.remove(name).expect("removing name that was never inserted");
                let check = self.check.remove(&at).expect("removing location that was never added");
                let stored = self.map.remove(at).context("could not get item")?;
                if check != stored {
                    Err(anyhow!(
                        "mismatch: expected {:?}, found {:?}",
                        BStr::new(check),
                        BStr::new(&stored)
                    ))
                } else {
                    Ok(())
                }
            }
            CheckItem::Check(name) => {
                let at = *self.names.get(name).expect("checking name that was never inserted");
                let check = *self.check.get(&at).expect("checking location that was never added");
                let stored = self.map.get(at).context("could not get item")?;
                if check != stored {
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
                println!("{:?}", BStr::new(b.trim_end_with(|b| b == '\0')));
                Ok(())
            }
        }
    }

    fn check_all(&mut self) -> anyhow::Result<()> {
        for name in self.names.keys().copied().collect::<Vec<_>>() {
            self.execute(CheckItem::Check(name))?;
        }
        Ok(())
    }
}
