use std::io::Cursor;

use varuint::{ReadVarint, VarintSizeHint, WriteVarint};

use crate::{Backing, Error};

pub(crate) fn write_varint_backing<N: VarintSizeHint + Copy>(n: N, backing: &mut Backing, position: &mut usize) -> Result<(), Error>
where
    for<'a> Cursor<&'a mut [u8]>: WriteVarint<N>,
{
    backing.resize_for(*position + n.varint_size())?;
    write_varint(n, &mut backing[..], position);
    Ok(())
}

pub(crate) fn write_varint<N: VarintSizeHint>(n: N, buffer: &mut [u8], position: &mut usize)
where
    for<'a> Cursor<&'a mut [u8]>: WriteVarint<N>,
{
    let mut cur = Cursor::new(buffer);
    cur.set_position(*position as u64);
    match cur.write_varint(n) {
        Ok(_) => *position = cur.position() as usize,
        Err(_) => unreachable!(),
    }
}

pub(crate) fn read_varint<T>(buffer: &[u8], position: &mut usize) -> Result<T, Error>
where
    for<'a> Cursor<&'a [u8]>: ReadVarint<T>,
{
    let mut cur = Cursor::new(buffer);
    cur.set_position(*position as u64);
    match cur.read_varint() {
        Ok(n) => {
            *position = cur.position() as usize;
            Ok(n)
        }
        Err(_) => Err(Error::InvalidVarint { position: *position }),
    }
}
