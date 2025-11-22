// build.rs
use std::env;
use std::path::Path;
use std::process::{Command, Stdio};

fn main() {
    // Re-run if this file changes.
    println!("cargo:rerun-if-changed=build.rs");

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| "unknown".into());
    let auto_install = env::var("HYBRID_CACHE_AUTO_INSTALL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // 1) Check meilisearch binary
    let meili_ok = command_exists("meilisearch");
    if !meili_ok {
        if auto_install {
            eprintln!("[build.rs] meilisearch not found, attempting auto-install via curl…");
            if let Err(e) = install_meilisearch(&target_os) {
                eprintln!("[build.rs] Failed to install meilisearch automatically: {e}");
            }
        } else {
            println!(
                "cargo:warning=meilisearch binary not found on PATH or at ./meilisearch. \
                 Set HYBRID_CACHE_AUTO_INSTALL=1 to let build.rs attempt an install \
                 (curl -L https://install.meilisearch.com | sh), \
                 or install meilisearch manually."
            );
        }
    }

    // 2) Check RocksDB dev libs (for system linking)
    let rocksdb_ok = check_rocksdb_via_pkg_config(&target_os);
    if !rocksdb_ok {
        if auto_install {
            eprintln!("[build.rs] RocksDB dev libs not found, attempting auto-install…");
            if let Err(e) = install_rocksdb(&target_os) {
                eprintln!("[build.rs] Failed to install RocksDB dev libs automatically: {e}");
            }
        } else {
            println!(
                "cargo:warning=RocksDB dev libs not found via pkg-config. \
                 Set HYBRID_CACHE_AUTO_INSTALL=1 to let build.rs attempt an install, \
                 or install librocksdb-dev / rocksdb-devel manually, \
                 or enable the `bundled` feature on the `rocksdb` crate."
            );
        }
    }
}

/// Returns true if the command exists or, for `meilisearch`, if `./meilisearch` exists.
fn command_exists(bin: &str) -> bool {
    // Try running `<bin> --version`
    if Command::new(bin)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        return true;
    }

    // Special case for Meilisearch installed via `curl | sh` which drops `./meilisearch`
    if bin == "meilisearch" && Path::new("./meilisearch").exists() {
        return true;
    }

    false
}

/// Check RocksDB dev libs via pkg-config when available.
fn check_rocksdb_via_pkg_config(target_os: &str) -> bool {
    if target_os != "macos" && target_os != "linux" {
        // On other platforms we just skip this check; you might be using bundled.
        return true;
    }

    let status = Command::new("pkg-config")
        .arg("--exists")
        .arg("rocksdb")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    matches!(status, Ok(s) if s.success())
}

/// Try to install Meilisearch via the official curl installer.
///
/// Equivalent to:
///   curl -L https://install.meilisearch.com | sh
///
/// This downloads the `meilisearch` binary into the current directory
/// (the crate root when build.rs runs). We **do not** run `./meilisearch`
/// here to avoid hanging the build.
fn install_meilisearch(target_os: &str) -> Result<(), String> {
    match target_os {
        "macos" | "linux" => {
            // Use a shell pipeline: curl -L ... | sh
            run_cmd(
                "sh",
                &["-c", "curl -L https://install.meilisearch.com | sh"],
            )?;
            Ok(())
        }
        other => Err(format!(
            "Auto-install for meilisearch via curl is not implemented for target_os={other}"
        )),
    }
}

/// Try to install RocksDB dev libs (for system linking).
fn install_rocksdb(target_os: &str) -> Result<(), String> {
    match target_os {
        "macos" => {
            if !command_exists("brew") {
                return Err("Homebrew not found; cannot install RocksDB automatically".into());
            }

            run_cmd("brew", &["update"])?;
            run_cmd("brew", &["install", "rocksdb"])?;
            Ok(())
        }

        "linux" => {
            if command_exists("apt-get") {
                run_cmd("sudo", &["apt-get", "update"])?;
                // Debian/Ubuntu package name
                run_cmd("sudo", &["apt-get", "install", "-y", "librocksdb-dev"])?;
                return Ok(());
            }

            if command_exists("yum") {
                // Amazon Linux / CentOS (package name may differ per repo)
                run_cmd("sudo", &["yum", "install", "-y", "rocksdb-devel"])?;
                return Ok(());
            }

            Err(
                "No known package manager (apt-get or yum) found to install RocksDB dev libs"
                    .into(),
            )
        }

        other => Err(format!(
            "Auto-install for RocksDB dev libs not implemented for target_os={other}"
        )),
    }
}

/// Helper to run a command and surface errors nicely.
fn run_cmd(bin: &str, args: &[&str]) -> Result<(), String> {
    eprintln!("[build.rs] Running: {bin} {}", args.join(" "));
    let status = Command::new(bin)
        .args(args)
        .status()
        .map_err(|e| format!("failed to spawn {bin}: {e}"))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("{bin} {:?} exited with status {}", args, status))
    }
}
