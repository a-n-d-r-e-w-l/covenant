use std::{
    num::NonZeroU64,
    path::Path,
    time::{Duration, Instant},
};

use cli_table::{Cell, Style, Table};
use kdam::BarExt;

use crate::data::{Data, Size};

pub mod data;

#[derive(Debug)]
pub struct Results(Vec<SizeResult>, Which);

impl Results {
    pub fn stdout(self) -> anyhow::Result<()> {
        let table = self
            .0
            .into_iter()
            .map(|r| {
                let this_inserts_win = r.this.inserts < r.sqlite.inserts;
                let this_reads_win = r.this.reads < r.sqlite.reads;

                vec![
                    r.size.cell(),
                    format!("{:.1}", r.bytes as f64 / r.keys as f64)
                        .cell()
                        .justify(cli_table::format::Justify::Right),
                    if self.1.this() {
                        format!("{:.2?}", r.this.inserts).cell().bold(this_inserts_win)
                    } else {
                        "N/A".cell().dimmed(true).italic(true)
                    }
                    .justify(cli_table::format::Justify::Right),
                    if self.1.sqlite() {
                        format!("{:.2?}", r.sqlite.inserts).cell().bold(!this_inserts_win)
                    } else {
                        "N/A".cell().dimmed(true).italic(true)
                    },
                    if self.1.this() {
                        format!("{:.2?}", r.this.reads).cell().bold(this_reads_win)
                    } else {
                        "N/A".cell().dimmed(true).italic(true)
                    }
                    .justify(cli_table::format::Justify::Right),
                    if self.1.sqlite() {
                        format!("{:.2?}", r.sqlite.reads).cell().bold(!this_reads_win)
                    } else {
                        "N/A".cell().dimmed(true).italic(true)
                    },
                ]
            })
            .collect::<Vec<_>>()
            .table()
            .title(vec![
                "Size",
                "Avg. key\nsize (bytes)",
                "covenant [write]",
                "sqlite [write]",
                "covenant [read]",
                "sqlite [read]",
            ])
            .dimmed(true);
        cli_table::print_stdout(table)?;
        Ok(())
    }
}

#[derive(Debug)]
struct SizeResult {
    size: Size,
    bytes: usize,
    keys: usize,
    this: RunResult,
    sqlite: RunResult,
}

#[derive(Debug, Default)]
struct RunResult {
    inserts: Duration,
    reads: Duration,
}

#[derive(Debug, Copy, Clone)]
pub enum Which {
    Both,
    This,
    Sqlite,
}

impl Which {
    fn this(self) -> bool {
        matches!(self, Self::Both | Self::This)
    }

    fn sqlite(self) -> bool {
        matches!(self, Self::Both | Self::Sqlite)
    }
}

pub fn run(dir: &Path, seeds: impl Iterator<Item = u64>, sizes: impl Iterator<Item = Size>, count: usize, which: Which) -> anyhow::Result<Results> {
    let mut sizes = sizes.collect::<Vec<_>>();
    sizes.sort();
    sizes.dedup();
    let seeds = seeds.collect::<Vec<_>>();
    let base_id = NonZeroU64::new(1).unwrap();

    let mut bar = kdam::Bar::new(sizes.len() * seeds.len());
    let mut results = Vec::with_capacity(sizes.len());
    for size in sizes {
        let mut this_acc = RunResult::default();
        let mut sqlite_acc = RunResult::default();
        let mut bytes_acc = 0;
        let mut keys_acc = 0;
        for &seed in &seeds {
            let keys = Data::new(size, seed, count);
            bytes_acc += keys.bytes();
            keys_acc += keys.count();
            fs_err::create_dir_all(dir)?;

            if which.this() {
                let mut lkp = unsafe { int_multistore::Lookup::new(dir.to_owned(), "bench") }?;

                let add_elapsed = {
                    let start = Instant::now();
                    for (i, key) in keys.into_iter().enumerate() {
                        let id = base_id.saturating_add(i as _);
                        if let Some(existing) = lkp.get_idx(key) {
                            lkp.insert(existing, key, id)?;
                        } else {
                            lkp.set(key, id)?;
                        }
                    }
                    start.elapsed()
                };

                let read_elapsed = {
                    let start = Instant::now();
                    for (i, key) in keys.into_iter().enumerate() {
                        let id = base_id.saturating_add(i as _);
                        let idx = lkp.get_idx(key).expect("key should be present");
                        assert!(lkp.get(idx).expect("valid idx").any(|j| j == id));
                    }
                    start.elapsed()
                };

                lkp.close()?;

                this_acc.inserts += add_elapsed;
                this_acc.reads += read_elapsed;
            };

            if which.sqlite() {
                let conn = rusqlite::Connection::open(dir.join("r.sqlite"))?;

                // Creating the index seems to speed up reads by about a factor of 2x
                // This doesn't have a comparison in the benchmark tables as we aren't benchmarking
                // sqlite against itself
                // If there's a way of speeding up sqlite, I'd be happy to change this
                conn.execute_batch(
                    r#"
                    CREATE TABLE lookup ( id INTEGER PRIMARY KEY, hash BLOB );
                    CREATE INDEX back ON lookup (hash);
                    "#,
                )?;

                let add_elapsed = {
                    let mut stmt = conn.prepare(
                        r#"
                        INSERT INTO lookup (id, hash) VALUES (?1, ?2);
                        "#,
                    )?;

                    let start = Instant::now();
                    for (i, key) in keys.into_iter().enumerate() {
                        stmt.execute((i, key))?;
                    }
                    start.elapsed()
                };

                let read_elapsed = {
                    let mut stmt = conn.prepare(
                        r#"
                        SELECT id FROM lookup WHERE hash = ?1;
                        "#,
                    )?;

                    let start = Instant::now();
                    'keys: for (i, key) in keys.into_iter().enumerate() {
                        let mut rows = stmt.query((key,))?;
                        while let Some(row) = rows.next()? {
                            if row.get::<_, usize>(0)? == i {
                                continue 'keys;
                            }
                        }
                        panic!("id not found in lookup");
                    }
                    start.elapsed()
                };

                if let Err((_, e)) = conn.close() {
                    return Err(e.into());
                }

                sqlite_acc.inserts += add_elapsed;
                sqlite_acc.reads += read_elapsed;
            };

            fs_err::remove_dir_all(dir)?;
            bar.update(1)?;
        }

        results.push(SizeResult {
            size,
            bytes: bytes_acc,
            keys: keys_acc,
            this: this_acc,
            sqlite: sqlite_acc,
        });
    }
    bar.clear()?;

    Ok(Results(results, which))
}
