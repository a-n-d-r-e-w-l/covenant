use crate::Id;
use anyhow::bail;
use binrw::{BinReaderExt, BinWriterExt};
use fs_err::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

type Stored = Id;

const WIDTH: u64 = 0_u64.to_be_bytes().len() as u64;

#[derive(Debug)]
pub(super) struct MultiIndexed {
    file: File,
    length: u64,
}

impl MultiIndexed {
    pub(super) fn from_file(mut file: File) -> anyhow::Result<Self> {
        let end = file.seek(SeekFrom::End(0))?;
        if end % WIDTH != 0 {
            bail!("invalid length: {end}, expected a multiple of {}", WIDTH)
        }
        Ok(Self { file, length: end })
    }

    pub(super) fn get(&mut self, offset: u64, length: u64) -> anyhow::Result<Vec<Stored>> {
        if offset + length * WIDTH > self.length {
            bail!(
                "attempted to index out of bounds: {} {} ({:?})",
                self.length,
                offset + length * WIDTH,
                (offset, length)
            )
        }
        self.file.seek(SeekFrom::Start(offset))?;

        let mut data = vec![0; (length * WIDTH) as usize];
        self.file.read_exact(&mut data)?;
        let mut cursor = Cursor::new(data);

        let items = (0..length).map(|_| cursor.read_be()).map(|r| r.map(Id)).collect::<Result<Vec<_>, _>>()?;
        Ok(items)
    }

    pub(super) fn append(&mut self, v: &[Stored]) -> anyhow::Result<(u64, u64)> {
        let mut data = Vec::new();
        let mut wtr = Cursor::new(&mut data);
        for item in v {
            wtr.write_be(&item.0)?;
        }
        assert_eq!(wtr.position(), data.len() as u64);
        let offset = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&data)?;
        assert_eq!(data.len(), v.len() * WIDTH as usize);
        let length = data.len() as u64;
        self.length += length;
        Ok((offset, length / WIDTH))
    }
}
