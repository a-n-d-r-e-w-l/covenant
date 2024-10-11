use std::fmt::{Display, Formatter};

use rand::{distributions::Distribution, RngCore, SeedableRng};

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[repr(u8)]
pub enum Size {
    Tiny = 0,
    Small,
    Medium,
    Large,
    Huge,
    Colossal,
}

impl Size {
    pub fn iter_inclusive(from: Self, to: Self) -> impl ExactSizeIterator<Item = Self> {
        assert!(from <= to);
        ((from as u8)..=(to as u8)).map(|i| unsafe { std::mem::transmute::<u8, Self>(i) })
    }

    pub fn range(self) -> std::ops::RangeInclusive<u16> {
        match self {
            Self::Tiny => 0..=7,
            Self::Small => 8..=127,
            Self::Medium => 128..=511,
            Self::Large => 512..=2047,
            Self::Huge => 2048..=8191,
            Self::Colossal => 20_480..=u16::MAX,
        }
    }
}

impl Display for Size {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Size::Tiny => "Tiny",
            Size::Small => "Small",
            Size::Medium => "Medium",
            Size::Large => "Large",
            Size::Huge => "Huge",
            Size::Colossal => "Colossal",
        })
    }
}

pub struct Data {
    lens: Vec<u16>,
    raw: Vec<u8>,
    rng: rand_pcg::Pcg64Mcg,
}

impl Data {
    pub(crate) fn new(size: Size, seed: u64, count: usize) -> Self {
        let mut rng = rand_pcg::Pcg64Mcg::seed_from_u64(seed);
        let range = size.range();
        let lens = rand::distributions::Uniform::new_inclusive(range.start(), range.end())
            .sample_iter(&mut rng)
            .take(count)
            .collect::<Vec<_>>();
        let total_len = lens.iter().map(|&l| l as usize).sum::<usize>();
        let mut raw = vec![0; total_len];
        rng.fill_bytes(&mut raw);
        Self { lens, raw, rng }
    }

    pub(crate) fn rng(&mut self) -> &mut rand_pcg::Pcg64Mcg {
        &mut self.rng
    }

    pub(crate) fn count(&self) -> usize {
        self.lens.len()
    }

    pub(crate) fn bytes(&self) -> usize {
        self.raw.len()
    }
}

impl<'a> IntoIterator for &'a Data {
    type Item = &'a [u8];
    type IntoIter = DataIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        DataIter {
            lens: &self.lens,
            raw: &self.raw,
        }
    }
}

pub struct DataIter<'a> {
    lens: &'a [u16],
    raw: &'a [u8],
}

impl<'a> Iterator for DataIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.lens.is_empty() {
            None
        } else {
            let first = self.lens[0];
            self.lens = &self.lens[1..];
            let b = &self.raw[..first as usize];
            self.raw = &self.raw[first as usize..];
            Some(b)
        }
    }
}
