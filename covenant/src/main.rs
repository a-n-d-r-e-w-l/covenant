use std::path::{Path, PathBuf};

use covenant::Ark;

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(run())
}

async fn run() -> anyhow::Result<()> {
    let _ = fs_err::tokio::remove_dir_all("test.ark").await;
    let mut ark = Ark::open(Path::new("test.ark/data"), Path::new("test.ark/objects")).await?;

    let paths = std::iter::once("LINKS.txt".into())
        // .chain(recursive_files("seqstore".as_ref()))
        .chain(std::iter::once("LINKS.txt".into()))
        .collect::<Vec<PathBuf>>();

    for path in &paths {
        let reader = fs_err::tokio::File::open(path).await?;
        ark.add(reader).await?;
    }
    ark.flush().await?;
    Ok(())
}

fn recursive_files(base: &Path) -> impl Iterator<Item = PathBuf> {
    fs_err::read_dir(base)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|d| {
            if let Ok(meta) = d.metadata() {
                if meta.is_dir() {
                    Some(Box::new(recursive_files(&d.path())) as Box<dyn Iterator<Item = PathBuf>>)
                } else {
                    Some(Box::new(std::iter::once(d.path())))
                }
            } else {
                None
            }
        })
        .flatten()
}
