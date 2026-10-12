//! Fetch a model's `config.json` from Hugging Face, with an on-disk cache.
//!
//! The file is downloaded from
//! `https://huggingface.co/<repo_id>/raw/<revision>/config.json`. If the
//! `HF_TOKEN` environment variable is set, it is sent as a bearer token so
//! gated repositories can be read.
//!
//! Fetched files are cached at `<cache>/<repo_id>/<revision>/config.json`,
//! where `<cache>` is the first of:
//!
//! 1. `$HF_HOME/cachesim/configs`
//! 2. `$XDG_CACHE_HOME/cachesim/hf-configs`
//! 3. `$HOME/.cache/cachesim/hf-configs`
//! 4. `%LOCALAPPDATA%\cachesim\hf-configs` (Windows)
//!
//! A cached file is used without contacting the network, so a branch name
//! such as `main` keeps resolving to the copy fetched first. Delete the cached
//! file, or pass a commit hash as the revision, to get a different version.
//!
//! Built only with the `hf-fetch` cargo feature (on by default).

use std::path::PathBuf;

use crate::Error;

/// Whether `s` is a non-empty name of ASCII letters, digits, `-`, `_` and
/// `.`, and not `.` or `..`.
fn valid_segment(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Check a repository id (`name` or `org/name`) and revision, since both
/// become URL and cache-path components.
pub fn validate(repo_id: &str, revision: &str) -> Result<(), Error> {
    let parts: Vec<&str> = repo_id.split('/').collect();
    if parts.len() > 2 || !parts.iter().all(|p| valid_segment(p)) {
        return Err(Error::Fetch(format!(
            "invalid Hugging Face repository id {repo_id:?}"
        )));
    }
    if !valid_segment(revision) {
        return Err(Error::Fetch(format!("invalid revision {revision:?}")));
    }
    Ok(())
}

/// Root of the config cache; see the [module documentation](self).
pub fn cache_dir() -> Option<PathBuf> {
    let env = |k: &str| {
        std::env::var_os(k)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    if let Some(p) = env("HF_HOME") {
        return Some(p.join("cachesim").join("configs"));
    }
    let base = env("XDG_CACHE_HOME")
        .or_else(|| env("HOME").map(|h| h.join(".cache")))
        .or_else(|| env("LOCALAPPDATA"))?;
    Some(base.join("cachesim").join("hf-configs"))
}

/// Path of the cached config for `repo_id` at `revision`.
pub fn cache_path(repo_id: &str, revision: &str) -> Result<PathBuf, Error> {
    validate(repo_id, revision)?;
    let mut p = cache_dir().ok_or_else(|| {
        Error::Fetch("no cache directory: set HF_HOME, XDG_CACHE_HOME or HOME".into())
    })?;
    p.extend(repo_id.split('/'));
    p.push(revision);
    p.push("config.json");
    Ok(p)
}

/// Return the bytes of `config.json` for `repo_id` at `revision`, from the
/// cache if present, otherwise downloaded and then cached.
pub fn fetch_config(repo_id: &str, revision: &str) -> Result<Vec<u8>, Error> {
    let path = cache_path(repo_id, revision)?;
    if let Ok(bytes) = std::fs::read(&path) {
        return Ok(bytes);
    }

    let url = format!("https://huggingface.co/{repo_id}/raw/{revision}/config.json");
    let mut req = ureq::get(&url);
    if let Some(token) = std::env::var("HF_TOKEN").ok().filter(|t| !t.is_empty()) {
        req = req.header("Authorization", &format!("Bearer {token}"));
    }
    let bytes = req
        .call()
        .and_then(|mut resp| resp.body_mut().read_to_vec())
        .map_err(|e| Error::Fetch(format!("{url}: {e}")))?;
    // Do not cache a body that is not JSON (an error or login page).
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .map_err(|e| Error::Fetch(format!("{url}: response is not JSON: {e}")))?;

    // Write to a temporary name and rename, so a concurrent reader never sees
    // a partial file.
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension(format!("json.{}.tmp", std::process::id()));
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, &path)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repo_ids_and_revisions_are_validated() {
        assert!(validate("Qwen/Qwen3-8B", "main").is_ok());
        assert!(validate("gpt2", "0123abcd").is_ok());
        assert!(validate("Qwen/Qwen3.8-27B", "v1.0").is_ok());
        for (repo, rev) in [
            ("", "main"),
            ("a/b/c", "main"),
            ("../etc", "main"),
            ("Qwen/..", "main"),
            ("Qwen/Qwen3-8B", ".."),
            ("Qwen/Qwen3-8B", "a/b"),
            ("Qwen/Qwen 3", "main"),
            ("Qwen/Qwen3-8B", "main?x=1"),
        ] {
            assert!(validate(repo, rev).is_err(), "{repo:?} {rev:?}");
        }
    }

    /// Needs network access to huggingface.co: `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn fetch_qwen3_8b() {
        let dir = tempfile::tempdir().unwrap();
        // Process-global env: this is the only test that sets HF_HOME, and it
        // is ignored by default.
        std::env::set_var("HF_HOME", dir.path());
        let bytes = fetch_config("Qwen/Qwen3-8B", "main").unwrap();
        let sizes = crate::mooncake::ModelSizes::from_config_bytes(&bytes, None).unwrap();
        assert_eq!(sizes.kv_bytes_per_token, 147_456);
        let cached = cache_path("Qwen/Qwen3-8B", "main").unwrap();
        assert!(cached.starts_with(dir.path()));
        assert_eq!(std::fs::read(cached).unwrap(), bytes);
        // Second call is served from the cache.
        assert_eq!(fetch_config("Qwen/Qwen3-8B", "main").unwrap(), bytes);
    }
}
