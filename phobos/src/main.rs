use kdam::TqdmIterator;
use phobos::Id;
use std::num::NonZeroU64;

fn main() -> anyhow::Result<()> {
    {
        let _ = fs_err::remove_dir_all("test.db");
    }
    big_test()?;
    Ok(())
}

fn big_test() -> anyhow::Result<()> {
    let mut db = phobos::Database::new("test.db".into(), "rust_DefaultHasher".to_owned())?;

    for i in (1..=1_000_000).tqdm() {
        db.add(format!("{:x}", i % 16).as_bytes(), Id::new(NonZeroU64::new(i).unwrap()))?;
    }
    // db.merge()?;
    db.unify_fsts()?;
    println!();

    // drop(db);
    let mut db = phobos::Database::new("test.db".into(), "rust_DefaultHasher".to_owned())?;
    for i in (1..=1_000_000).tqdm() {
        let r = db.get(format!("{:x}", i % 16).as_bytes())?.expect("key should exist");
        r.binary_search(&Id::new(NonZeroU64::new(i).unwrap()))
            .expect("id should be associated with key");
    }
    println!();

    Ok(())
}
