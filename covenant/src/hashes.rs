use std::ops::{Index, IndexMut};

use digest::{Digest, Output};

#[derive(Debug)]
pub(crate) struct Hashes {
    md5: [u8; 16],
    sha1: [u8; 20],
    sha2: [u8; 32],
    sha3: [u8; 32],
    blake2b: [u8; 64],
    blake3: [u8; 32],
}

fn hash<H: Digest, const N: usize>(b: &[u8]) -> [u8; N]
where
    [u8; N]: From<Output<H>>,
{
    H::new().chain_update(b).finalize().into()
}

impl Hashes {
    pub(crate) fn extract(b: &[u8]) -> anyhow::Result<Self> {
        let md5 = md5::compute(b).0;
        let sha1 = hash::<sha1::Sha1, 20>(b);
        let sha2 = hash::<sha2::Sha256, 32>(b);
        let sha3 = hash::<sha3::Sha3_256, 32>(b);
        let blake2b = hash::<blake2::Blake2b512, 64>(b);
        let blake3 = blake3::hash(b).into();

        Ok(Self {
            md5,
            sha1,
            sha2,
            sha3,
            blake2b,
            blake3,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HashesMap<V>([V; 6]);

impl<V> HashesMap<V> {
    pub(crate) fn new_with(mut f: impl FnMut(HashKind) -> V) -> Self {
        Self([
            f(HashKind::MD5),
            f(HashKind::SHA1),
            f(HashKind::SHA2),
            f(HashKind::SHA3),
            f(HashKind::Blake2b),
            f(HashKind::Blake3),
        ])
    }

    pub(crate) fn try_new_with<E>(mut f: impl FnMut(HashKind) -> Result<V, E>) -> Result<Self, E> {
        Ok(Self([
            f(HashKind::MD5)?,
            f(HashKind::SHA1)?,
            f(HashKind::SHA2)?,
            f(HashKind::SHA3)?,
            f(HashKind::Blake2b)?,
            f(HashKind::Blake3)?,
        ]))
    }

    pub(crate) fn new_from(v: V) -> Self
    where
        V: Clone,
    {
        Self([v.clone(), v.clone(), v.clone(), v.clone(), v.clone(), v])
    }
}

impl<V> Index<HashKind> for HashesMap<V> {
    type Output = V;

    fn index(&self, index: HashKind) -> &Self::Output {
        &self.0[index as u8 as usize]
    }
}

impl<V> IndexMut<HashKind> for HashesMap<V> {
    fn index_mut(&mut self, index: HashKind) -> &mut Self::Output {
        &mut self.0[index as u8 as usize]
    }
}

fn into_iter_map<V>((i, v): (usize, &mut V)) -> (HashKind, &mut V) {
    (HashKind::from_idx(i), v)
}

impl<'a, V> IntoIterator for &'a mut HashesMap<V> {
    type Item = (HashKind, &'a mut V);
    type IntoIter = std::iter::Map<std::iter::Enumerate<std::slice::IterMut<'a, V>>, fn((usize, &mut V)) -> (HashKind, &mut V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut().enumerate().map(into_iter_map)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum HashKind {
    MD5,
    SHA1,
    SHA2,
    SHA3,
    Blake2b,
    Blake3,
}

impl HashKind {
    #[inline]
    fn from_idx(i: usize) -> Self {
        match i as u8 {
            0 => Self::MD5,
            1 => Self::SHA1,
            2 => Self::SHA2,
            3 => Self::SHA3,
            4 => Self::Blake2b,
            5 => Self::Blake3,
            _ => unreachable!(),
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::MD5 => "md5",
            Self::SHA1 => "sha1",
            Self::SHA2 => "sha2",
            Self::SHA3 => "sha3",
            Self::Blake2b => "blake2b",
            Self::Blake3 => "blake3",
        }
    }
}

impl<'a> IntoIterator for &'a Hashes {
    type Item = (HashKind, &'a [u8]);
    type IntoIter = std::array::IntoIter<(HashKind, &'a [u8]), 6>;

    fn into_iter(self) -> Self::IntoIter {
        [
            (HashKind::MD5, self.md5.as_ref()),
            (HashKind::SHA1, self.sha1.as_ref()),
            (HashKind::SHA2, self.sha2.as_ref()),
            (HashKind::SHA3, self.sha3.as_ref()),
            (HashKind::Blake2b, self.blake2b.as_ref()),
            (HashKind::Blake3, self.blake3.as_ref()),
        ]
        .into_iter()
    }
}
