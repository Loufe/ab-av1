//! temp file logic
use std::{
    collections::HashMap,
    env, iter,
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
};

static TEMPS: LazyLock<Mutex<HashMap<PathBuf, TempKind>>> = LazyLock::new(<_>::default);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TempKind {
    /// Should always be deleted at the end of the program.
    NotKeepable,
    /// Usually deleted but may be kept, e.g. with --keep.
    Keepable,
}

/// Add a file as temporary so it can be deleted later.
pub fn add(file: impl Into<PathBuf>, kind: TempKind) {
    TEMPS.lock().unwrap().insert(file.into(), kind);
}

/// Remove a previously added file so that it won't be deleted later,
/// if it hasn't already.
pub fn unadd(file: &Path) -> bool {
    TEMPS.lock().unwrap().remove(file).is_some()
}

/// Delete all added temporary files.
/// If `keep_keepables` true don't delete [`TempKind::Keepable`] temporary files.
pub async fn clean(keep_keepables: bool) -> anyhow::Result<()> {
    match keep_keepables {
        true => clean_non_keepables().await,
        false => clean_all().await,
    }
}

/// Delete all added temporary files.
pub async fn clean_all() -> anyhow::Result<()> {
    let files = std::mem::take(&mut *TEMPS.lock().unwrap())
        .into_iter()
        .collect();
    remove_files(files).await
}

async fn clean_non_keepables() -> anyhow::Result<()> {
    let files = {
        let mut temps = TEMPS.lock().unwrap();
        let matching: Vec<_> = temps
            .iter()
            .filter(|(_, kind)| **kind == TempKind::NotKeepable)
            .map(|(file, _)| file.clone())
            .collect();
        matching
            .into_iter()
            .filter_map(|file| temps.remove_entry(&file))
            .collect()
    };
    remove_files(files).await
}

async fn remove_files(mut files: Vec<(PathBuf, TempKind)>) -> anyhow::Result<()> {
    files.sort_by_key(|(file, _)| file.is_dir()); // rm dir at the end
    let mut failed = Vec::new();
    let mut errors = Vec::new();

    for (file, kind) in files {
        let removed = match file.is_dir() {
            true => tokio::fs::remove_dir(&file).await,
            false => tokio::fs::remove_file(&file).await,
        };
        if let Err(error) = removed
            && error.kind() != std::io::ErrorKind::NotFound
        {
            errors.push(format!("{}: {error}", file.display()));
            failed.push((file, kind));
        }
    }

    TEMPS.lock().unwrap().extend(failed);
    if errors.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("failed to remove temporary files:\n{}", errors.join("\n"))
    }
}

/// Return a temporary directory that is distinct per process/run.
///
/// Configured --temp-dir is used as a parent or, if not set, the current working dir.
pub fn process_dir(conf_parent: Option<PathBuf>) -> PathBuf {
    static SUBDIR: LazyLock<String> = LazyLock::new(|| {
        let mut subdir = String::from(".ab-av1-");
        subdir.extend(iter::repeat_with(fastrand::alphanumeric).take(12));
        subdir
    });

    let mut temp_dir =
        conf_parent.unwrap_or_else(|| env::current_dir().expect("current working directory"));
    temp_dir.push(&*SUBDIR);

    if !temp_dir.exists() {
        add(&temp_dir, TempKind::Keepable);
        std::fs::create_dir_all(&temp_dir).expect("failed to create temp-dir");
    }

    temp_dir
}

#[cfg(test)]
mod tests {
    use super::{TempKind, remove_files};

    #[tokio::test]
    async fn cleanup_removes_files_and_accepts_missing_files() {
        let file = std::env::temp_dir().join(format!("ab-av1-cleanup-test-{}", fastrand::u64(..)));
        tokio::fs::write(&file, b"temporary")
            .await
            .expect("create temporary test file");
        let missing = file.with_extension("missing");

        remove_files(vec![
            (file.clone(), TempKind::NotKeepable),
            (missing, TempKind::NotKeepable),
        ])
        .await
        .expect("clean temporary test files");

        assert!(!file.exists());
    }
}
