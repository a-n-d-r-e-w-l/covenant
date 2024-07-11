use std::{
    fmt::{Debug, Formatter},
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::Context;
use fs4::FileExt;

pub(crate) struct Lock {
    file: File,
    path: PathBuf,
}

impl Lock {
    pub(crate) fn new(at: &Path) -> anyhow::Result<Self> {
        let lock_file = fs_err::OpenOptions::new().read(true).write(true).create(true).open(at)?.into_parts().0;
        lock_file.try_lock_exclusive().context(format!("could not lock {}", at.display()))?;
        Ok(Self {
            file: lock_file,
            path: at.to_owned(),
        })
    }

    fn unlock(&mut self) -> anyhow::Result<()> {
        self.file.unlock().context(format!("could not unlock {}", self.path.display()))?;
        Ok(())
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        // TODO: logging
        let _ = self.unlock();
    }
}

impl Debug for Lock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lock").field("path", &self.path).finish_non_exhaustive()
    }
}
