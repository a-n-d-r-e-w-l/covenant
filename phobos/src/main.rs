use bytes::Bytes;

fn main() -> anyhow::Result<()> {
    let _ = fs_err::remove_dir_all("test.db");
    {
        let mut db = unsafe { phobos::Database::builder("test.db".into(), "hex".to_owned()).create(true).open() }?;

        for i in 1..=10_000 {
            db.set(Bytes::from(format!("{:x}", i)), i)?;
        }
        // db.flush()?;
        db.merge()?;
    }

    {
        let mut db = unsafe { phobos::Database::builder("test.db".into(), "hex".to_owned()).create(false).open() }?;
        for i in 1..=10_000 {
            let r = db.get(format!("{:x}", i).as_bytes()).expect("key should exist");
            assert_eq!(i, r);
        }
    }
    Ok(())
}
