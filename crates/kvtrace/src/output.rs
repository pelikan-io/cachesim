//! Output file handling shared by the converters: refuse outputs that must
//! not be replaced, and write through a temporary file that is renamed into
//! place only on success.

use std::path::{Path, PathBuf};

use crate::Error;

/// "a directory", "a pipe", ... for an error message.
pub fn describe_file_type(meta: &std::fs::Metadata) -> &'static str {
    let ft = meta.file_type();
    if ft.is_dir() {
        return "a directory";
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        if ft.is_fifo() {
            return "a pipe";
        }
        if ft.is_socket() {
            return "a socket";
        }
        if ft.is_char_device() || ft.is_block_device() {
            return "a device";
        }
    }
    "not a regular file"
}

/// Where a converter writes its output.
#[derive(Debug, Clone)]
pub struct OutputPlan {
    /// The file the result ends up in: the output path, or the file it links to.
    pub dest: PathBuf,
    /// Temporary file in `dest`'s directory, renamed over `dest` on success;
    /// `None` when `dest` is a device or pipe that is written directly.
    pub temp: Option<PathBuf>,
}

impl OutputPlan {
    /// The file to write: the temporary file, or `dest` when there is none.
    pub fn target(&self) -> &Path {
        self.temp.as_deref().unwrap_or(&self.dest)
    }

    /// After a successful write: rename the temporary file over `dest`. On
    /// error the temporary file is removed.
    pub fn commit(&self) -> Result<(), Error> {
        let Some(temp) = &self.temp else {
            return Ok(());
        };
        std::fs::rename(temp, &self.dest).map_err(|e| {
            let _ = std::fs::remove_file(temp);
            Error::from(e)
        })
    }

    /// After a failed write: remove the temporary file, if any.
    pub fn discard(&self) {
        if let Some(temp) = &self.temp {
            let _ = std::fs::remove_file(temp);
        }
    }
}

/// Distinguishes temporary files of concurrent conversions in one process.
static TEMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Decide where the output is written, refusing outputs that must not be
/// replaced, before any work is done.
///
/// * A symlink is followed: the result is written to the file it links to,
///   as writing through the link would. A dangling link is an error.
/// * The input file, a directory, or an existing read-only file is an error.
/// * A device or pipe (e.g. `/dev/null`) is written directly.
/// * Otherwise the result goes to `.<name>.<pid>.<n>.tmp` beside the
///   destination and is renamed over it on success.
pub fn plan_output(
    input: &Path,
    input_meta: &std::fs::Metadata,
    output: &Path,
) -> Result<OutputPlan, Error> {
    let refuse = |what: &str| Error::InvalidFormat(format!("output {} {what}", output.display()));
    let is_link = std::fs::symlink_metadata(output).is_ok_and(|m| m.file_type().is_symlink());
    let dest = if is_link {
        std::fs::canonicalize(output)
            .map_err(|e| refuse(&format!("is a symlink that cannot be resolved: {e}")))?
    } else {
        output.to_path_buf()
    };
    if same_file(input, input_meta, &dest) {
        return Err(refuse("is the input file"));
    }
    match std::fs::metadata(&dest) {
        Ok(m) if m.is_dir() => return Err(refuse("is a directory")),
        Ok(m) if !m.is_file() => return Ok(OutputPlan { dest, temp: None }),
        Ok(m) if m.permissions().readonly() => return Err(refuse("exists and is read-only")),
        _ => {}
    }
    let name = dest
        .file_name()
        .ok_or_else(|| refuse("has no file name"))?
        .to_string_lossy()
        .into_owned();
    let n = TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temp = dest.with_file_name(format!(".{name}.{}.{n}.tmp", std::process::id()));
    Ok(OutputPlan {
        dest,
        temp: Some(temp),
    })
}

/// Whether `output` names the input file: the same path after resolving
/// symlinks, or (on Unix) the same device and inode, which also catches hard
/// links.
pub fn same_file(input: &Path, input_meta: &std::fs::Metadata, output: &Path) -> bool {
    let Ok(out_meta) = std::fs::metadata(output) else {
        return false; // the output does not exist yet
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if input_meta.dev() == out_meta.dev() && input_meta.ino() == out_meta.ino() {
            return true;
        }
    }
    let _ = (input_meta, out_meta);
    matches!(
        (std::fs::canonicalize(input), std::fs::canonicalize(output)),
        (Ok(a), Ok(b)) if a == b
    )
}
