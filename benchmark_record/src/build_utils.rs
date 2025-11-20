use std::env;
use std::collections::HashMap;

// TODO: Record package versions, rust edition and rust compiler information here


/// Records compile-time information by capturing into into environment variables.
/// This function should call all of the functions needed to record build time information.
/// This is likely everything that is made with the define_env_vars! macro.
pub fn record_build_time_info() {
    record_cargo_env_vars();
    record_git_info();
}


#[macro_export]
macro_rules! embed_build_time_info {
    ($bench:expr) => {
        benchmark_record::embed_compile_info!($bench);
        benchmark_record::embed_git_info!($bench);
    };
}

/// Macro to define both a const array and a corresponding compile-time capture macro.
/// 
/// This macro generates:
/// 1. A public const array containing the variable names (for use in build.rs)
/// 2. A macro that captures those variables at compile time using option_env!
/// 
/// # Example
/// ```
/// define_env_vars!(
///     MY_VARS,           // Name of the const array
///     embed_my_vars,   // Name of the capture macro
///     with_compile_info, // Name of the BenchmarkInformation method to call
///     [
///         "CARGO_PKG_NAME",
///         "PROFILE",
///     ]
/// );
/// 
/// // Usage in build.rs:
/// capture_my_vars();
/// 
/// // Usage in main code:
/// let mut bench = BenchmarkInformation::new();
/// embed_my_vars!(bench);
/// ```
macro_rules! define_env_vars {
    ($const_name:ident, $macro_name:ident, $record_fn:ident, [$($var_name:literal),* $(,)?]) => {
        /// A set of environment variable names to record
        pub const $const_name: &[&str] = &[
            $($var_name,)*
        ];
        
        /// Macro to capture the compile-time values of the defined environment variables
        #[macro_export]
        macro_rules! $macro_name {
            ($bench:expr) => {
                $(
                    $bench.$record_fn(
                        $var_name,
                        option_env!($var_name)
                            .unwrap_or(concat!($var_name, " variable not set"))
                            .to_string()
                    );
                )*
            };
        }
    };
}

// Define the default set of environment variables to capture
define_env_vars!(
    DEFAULT_ENV_VARS,
    embed_compile_info,
    with_compile_info,
    [
        "CARGO_CFG_FEATURE",
        "CARGO_CFG_TARGET_ARCH",
        "CARGO_CFG_TARGET_OS",
        "CARGO_CFG_TARGET_FEATURE",
        "CARGO_CFG_TARGET_HAS_ATOMIC",
        "CARGO_CFG_TARGET_POINTER_WIDTH",
        "CARGO_PKG_NAME",
        "CARGO_PKG_VERSION",
        "PROFILE",
        "OPT_LEVEL",
    ]
);

/// For each variable name given in `var_names`, if the environment variable is set,
/// passes it through to rustc via cargo:rustc-env so it can be accessed with option_env!
pub fn record_cargo_env_vars() {
    for &var_name in DEFAULT_ENV_VARS {
        if let Ok(var_value) = env::var(var_name) {
            println!("cargo:rustc-env={}={}", var_name, var_value);
        }
    }
}

define_env_vars!(
    GIT_ENV_VARS,
    embed_git_info,
    with_git_info,
    [
        "GIT_commit_hash",
        "GIT_short_hash",
        "GIT_commit_date",
        "GIT_commit_message",
        "GIT_status",
    ]
);

/// Records git information as environment variables (which may then be visible at compile-time)
pub fn record_git_info() -> HashMap<String, String> {
    let mut git_info = HashMap::new();

    // Get long hash
    if let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        && output.status.success()
    {
        let commit_hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
        git_info.insert("GIT_commit_hash".to_string(), commit_hash);
    }

    // Get short hash
    if let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        && output.status.success()
    {
        let short_hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
        git_info.insert("GIT_short_hash".to_string(), short_hash);
    }

    // Get commit date
    if let Ok(output) = std::process::Command::new("git")
        .args(["log", "-1", "--format=%cd", "--date=iso"])
        .output()
        && output.status.success()
    {
        let commit_date = String::from_utf8_lossy(&output.stdout).trim().to_string();
        git_info.insert("GIT_commit_date".to_string(), commit_date);
    }

    // Get commit message
    if let Ok(output) = std::process::Command::new("git")
        .args(["log", "-1", "--format=%s"])
        .output()
        && output.status.success()
    {
        let commit_message = String::from_utf8_lossy(&output.stdout).trim().to_string();
        git_info.insert("GIT_commit_message".to_string(), commit_message);
    }

    // Get git status in short format
    if let Ok(output) = std::process::Command::new("git")
        .args(["status", "-s"])
        .output()
        && output.status.success()
    {
        let git_status_output = String::from_utf8_lossy(&output.stdout);
        let git_status = git_status_output
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .collect::<Vec<&str>>()
            .join(", ");
        git_info.insert("GIT_status".to_string(), git_status);
    } else {
        // If git command fails, store empty string
        git_info.insert("GIT_status".to_string(), "command failed".to_string());
    }


    for entry in GIT_ENV_VARS.iter() {
        if !git_info.contains_key(&entry.to_string()) {
            panic!("Missing git info key: {}", entry);
        }
    }

    for (key, value) in git_info.iter() {
        if !GIT_ENV_VARS.contains(&key.as_str()) {
            panic!("Unexpected git info key: {}", key);
        }
        println!("cargo:rustc-env={}={}", key, value);
    }

    git_info
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_git_info() {
        let git_info = record_git_info();

        println!("Git info: {:?}", git_info);

        // Check that all expected git fields are present
        assert!(
            git_info.contains_key("commit_hash"),
            "commit_hash should be present"
        );
        assert!(
            git_info.contains_key("short_hash"),
            "short_hash should be present"
        );
        assert!(
            git_info.contains_key("commit_date"),
            "commit_date should be present"
        );
        assert!(
            git_info.contains_key("commit_message"),
            "commit_message should be present"
        );

        // Check that values are not empty (assuming we're in a git repository)
        if let Some(commit_hash) = git_info.get("commit_hash") {
            assert!(!commit_hash.is_empty(), "commit_hash should not be empty");
            assert!(
                commit_hash.len() >= 40,
                "commit_hash should be at least 40 characters"
            );
        }

        if let Some(short_hash) = git_info.get("short_hash") {
            assert!(!short_hash.is_empty(), "short_hash should not be empty");
            assert!(
                short_hash.len() >= 7,
                "short_hash should be at least 7 characters"
            );
        }

        if let Some(commit_date) = git_info.get("commit_date") {
            assert!(!commit_date.is_empty(), "commit_date should not be empty");
        }

        if let Some(commit_message) = git_info.get("commit_message") {
            assert!(
                !commit_message.is_empty(),
                "commit_message should not be empty"
            );
        }
    }
}