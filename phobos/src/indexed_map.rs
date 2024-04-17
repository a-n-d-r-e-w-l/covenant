use crate::InternalId;
use anyhow::bail;
use binrw::{BinRead, BinWrite};
use fs_err::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

type Stored = (u64, u64); // offset, # of items

const WIDTH: u64 = (0_u64.to_be_bytes().len() * 2) as u64;

#[derive(Debug)]
pub(super) struct IndexedMap {
    count: u64,
    file: File,
}

impl IndexedMap {
    pub(super) fn from_file(mut file: File) -> anyhow::Result<Self> {
        let end = file.seek(SeekFrom::End(0))?;
        if end % WIDTH != 0 {
            bail!("invalid length: {end}, expected a multiple of {}", WIDTH)
        }
        let count = end / WIDTH;
        Ok(Self { count, file })
    }

    pub(super) fn get(&mut self, idx: InternalId) -> anyhow::Result<Option<Stored>> {
        if idx.0 >= self.count {
            return Ok(None);
        }
        self.file.seek(SeekFrom::Start(idx.0 * WIDTH))?;

        let mut data = vec![0; WIDTH as usize];
        self.file.read_exact(&mut data)?;

        let v = Stored::read_be(&mut Cursor::new(data))?;

        Ok(Some(v))
    }

    pub(super) fn set(&mut self, idx: InternalId, v: &Stored) -> anyhow::Result<()> {
        assert!(idx.0 <= self.count);
        if idx.0 == self.count {
            self.count += 1;
        }
        self.file.seek(SeekFrom::Start(idx.0 * WIDTH))?;
        let mut data = vec![0; WIDTH as usize];
        v.write_be(&mut Cursor::new(&mut data))?;
        self.file.write_all(&data)?;
        Ok(())
    }
}
