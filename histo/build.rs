// build.rs is a script that cargo executes before compiling the package
// usually used to link external libraries, generate code, or compile non-rust code
// for this project we will use it to include the git crate

use std::process::Command;

fn main() {
    // Make Cargo rerun the build script when HEAD changes.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");

    // Try to get the current commit hash
    let commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "unknown".into());

    // Expose the commit hash as a compile-time env var
    println!("cargo:rustc-env=GIT_COMMIT_HASH={commit}");
}

