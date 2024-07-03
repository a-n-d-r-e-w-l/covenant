use crate::{backing::BackingInner, error::Error, raw_store::RawStore, tag::MagicTag, Backing, Id};

impl RawStore {
    pub fn filter(&self, to: Backing) -> Result<Filter<'_>, Error> {
        Filter::new(self, to)
    }
}

#[derive(Debug)]
pub struct Filter<'a> {
    store: &'a RawStore,
    to: BackingInner,
}

impl<'a> Filter<'a> {
    fn new(store: &'a RawStore, to: Backing) -> Result<Self, Error> {
        let mut to = to.0;
        to.resize_for(store.header_length)?;
        to[..store.header_length].copy_from_slice(&store.backing[..store.header_length]);
        Ok(Self { store, to })
    }

    pub fn add(&mut self, at: Id) -> Result<(), Error> {
        let mut position = at.at();
        let tag = MagicTag::read(&self.store.backing, &mut position)?;
        match tag {
            MagicTag::Writing { .. } => Err(Error::EntryCorrupt { position: at.at() }),
            MagicTag::Written { length } => {
                at.verify(length)?;
                self.to.resize_for(position + length as usize)?;
                let b = &self.store.backing[at.at()..position + length as usize];
                self.to[at.at()..position + length as usize].copy_from_slice(b);
                Ok(())
            }
            other => Err(Error::IncorrectTag {
                position: at.at(),
                found: other.into(),
                expected_kind: "Written",
            }),
        }
    }

    pub fn finish(mut self) -> Result<(), Error> {
        let mut position = self.store.header_length;
        loop {
            if position >= self.to.len() {
                break;
            }
            if self.to[position] == 0 {
                let zero_run = self.to[position..].iter().take_while(|&&b| b == 0).count();
                if position + zero_run >= self.to.len() {
                    break;
                }
                let (tag_len, len) = MagicTag::calc_tag_len(zero_run);
                MagicTag::Deleted { length: len as u64 }.write_exact(&mut self.to, &mut position, tag_len as usize)?;
                position += len;
            } else {
                let tag = MagicTag::read(&self.to, &mut position)?;
                let MagicTag::Written { length } = tag else {
                    unreachable!("only Written tags are copied across")
                };
                position += length as usize;
            }
        }
        MagicTag::End.write(&mut self.to, &mut position)?;
        self.to.flush()?;
        Ok(())
    }
}
