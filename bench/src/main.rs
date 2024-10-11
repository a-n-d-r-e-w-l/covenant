use std::path::Path;

use bench::{data::Size, Which};

fn main() -> anyhow::Result<()> {
    let target_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("storage");
    if target_dir.exists() {
        fs_err::remove_dir_all(&target_dir)?;
    }
    let results = bench::run(
        &target_dir,
        0..1,
        Size::iter_inclusive(Size::Medium, Size::Medium),
        50_000,
        Which::Sqlite,
    )?;
    results.stdout()?;

    Ok(())
}
