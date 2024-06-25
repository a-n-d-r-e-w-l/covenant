use bytes::Bytes;

fn main() -> anyhow::Result<()> {
    {
        let _ = fs_err::remove_dir_all("test.db");
    }
    big_test()?;
    Ok(())
}

fn big_test() -> anyhow::Result<()> {
    {
        let mut db = phobos::Database::new("test.db".into(), "hex".to_owned())?;

        for i in 1..=10_000 {
            db.set(Bytes::from(format!("{:x}", i)), i)?;
        }
        // db.merge()?;
        db.unify_fsts()?;
    }

    {
        let mut db = phobos::Database::new("test.db".into(), "hex".to_owned())?;
        for i in 1..=10_000 {
            let r = db.get(format!("{:x}", i).as_bytes()).expect("key should exist");
            assert_eq!(i, r);
        }
    }

    Ok(())
}
