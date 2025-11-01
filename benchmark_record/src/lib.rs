use json::JsonValue;
use std::collections::HashMap;
use std::env;
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

const CHECK_PACKAGES: [&str; 4] = ["lamellar", "rofi", "rofisys", "lamellar-impl"];

pub struct BenchmarkInformation {
    pub benchmark_name: String,
    executable: PathBuf,
    parameters: Vec<String>,
    run_date: String,
    output: HashMap<String, String>,
    build_type: String,
    package_info: HashMap<String, String>,
    git: HashMap<String, String>,
    slurm_params: HashMap<String, String>,
    system: HashMap<String, String>,
    environment_vars: HashMap<String, String>,
    rust_edition: String,
    rust_compiler: String,
}

impl BenchmarkInformation {
    /// Create a new BenchmarkInformation instance with default benchmark name.
    /// This is the suggested way to construct a benchmark information record.
    pub fn new() -> Self {
        let benchmark_name = default_benchmark_name();
        Self::with_name(&benchmark_name)
    }

    /// Create a new BenchmarkInformation instance with the given benchmark name
    pub fn with_name(benchark_name: &str) -> Self {
        let executable_name = executable();
        Self {
            benchmark_name: benchark_name.to_string(),
            executable: executable_name,
            parameters: env::args().skip(1).collect(),
            run_date: BenchmarkInformation::get_run_date(),
            output: HashMap::new(),
            build_type: BenchmarkInformation::get_build_type(),
            package_info: BenchmarkInformation::get_package_info(),
            git: BenchmarkInformation::get_git_info(),
            slurm_params: BenchmarkInformation::collect_env_vars("SLURM"),
            system: BenchmarkInformation::get_system_info(),
            environment_vars: BenchmarkInformation::collect_env_vars("LAMELLAR"),
            rust_edition: BenchmarkInformation::get_rust_edition(),
            rust_compiler: BenchmarkInformation::get_rust_compiler(),
        }
    }

    /// Add a key/value pair to the output section of the benchmark information.
    pub fn with_output(&mut self, key: &str, value: String) {
        self.output.insert(key.to_string(), value);
    }

    /// Convert the captured information into a JsonValue object.
    /// This is not intended as a stable API, but may be useful for some cases...use with caution.
    pub fn as_json(&self) -> JsonValue {
        json::object! {
            "benchark name" => self.benchmark_name.clone(),
            "executable" => self.executable.to_string_lossy().to_string(),
            "parameters" => self.parameters.clone(),
            "run_date" => self.run_date.clone(),
            "output" => self.output.clone(),
            "build type" => self.build_type.clone(),
            "dependencies" => self.package_info.clone(),
            "git" => self.git.clone(),
            "system" => self.system.clone(),
            "environment" => self.environment_vars.clone(),
            "slurm_params" => self.slurm_params.clone(),
            "rust_edition" => self.rust_edition.clone(),
            "rust_compiler" => self.rust_compiler.clone(),
        }
    }

    /// Display the captured information in JSON format to stdout.
    /// If `indent` is `Some(u16)`, pretty-prints with the given indentation level.
    /// If `indent` is `None`, prints in compact form as a single line.
    pub fn display(&self, indent: Option<u16>) {
        let json_obj = self.as_json();
        match indent {
            None => println!("{}", json::stringify(json_obj)),
            Some(indent) => println!("{}", json::stringify_pretty(json_obj, indent)),
        }
    }

    /// Write the captured information to a specified file in JSON format.
    /// Assumes the file is in JSON-lines format and appends to the end.
    /// Will create the file if it does not exist, appends to it if it does.
    pub fn write(&self, file: &PathBuf) {
        let json_obj = self.as_json();

        // Try to create parent directories
        if let Some(parent) = file.parent() {
            let _ = fs::create_dir_all(parent);
        }

        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(file) {
            let _ = writeln!(f, "{}", json::stringify(json_obj));
        }
    }

    /// Generate a default output file name based on the benchmark name and slurm ID or current time.
    pub fn default_output_path(&self, root: &str) -> PathBuf {
        let stem = self.benchmark_name.clone();
        let time = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
        let id = self.slurm_params.get("SLURM_JOB_ID").unwrap_or(&time);        
        PathBuf::from(format!("{root}/{stem}_{id}_result.jsonl"))
    }


    /// Collects all environment variables that start with the given prefix into a HashMap
    fn collect_env_vars(prefix: &str) -> HashMap<String, String> {
        env::vars()
            .filter(|(key, _)| key.starts_with(prefix))
            .collect()
    }

    fn get_run_date() -> String {
        let datetime = chrono::Local::now();
        datetime.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    /// If in a standard build context, will be the parent dir.  Else unknown...
    fn get_build_type() -> String {
        let exec = executable();
        let alt_name = PathBuf::from("<unknown>");
        let parent = exec.parent().unwrap_or(alt_name.as_path());
        let build_type = parent
            .file_name()
            .unwrap_or(OsStr::new("<unknown>"))
            .to_string_lossy()
            .to_string();
        if ["debug", "release"].contains(&build_type.as_str()) {
            build_type
        } else {
            "<unknown>".to_string()
        }
    }

    /// Gathers selected system information using the sysinfo crate.
    fn get_system_info() -> HashMap<String, String> {
        let mut system_info = HashMap::new();
        let sys = sysinfo::System::new_all();

        if let Some(os_name) = sysinfo::System::name() {
            system_info.insert("os_name".to_string(), os_name);
        }
        if let Some(kernel_version) = sysinfo::System::kernel_version() {
            system_info.insert("kernel_version".to_string(), kernel_version);
        }
        if let Some(os_version) = sysinfo::System::long_os_version() {
            system_info.insert("os_version".to_string(), os_version);
        }
        if let Some(hostname) = sysinfo::System::host_name() {
            system_info.insert("hostname".to_string(), hostname);
        }

        system_info.insert("cpu_cores".to_string(), sys.cpus().len().to_string());
        system_info.insert(
            "physical_cpu_cores".to_string(),
            sysinfo::System::physical_core_count()
                .unwrap_or(0)
                .to_string(),
        );
        let cpu = &sys.cpus()[0];
        system_info.insert("cpu_frequency_mhz".to_string(), cpu.frequency().to_string());
        system_info.insert("cpu_vendor_id".to_string(), cpu.vendor_id().to_string());
        system_info.insert("cpu_brand".to_string(), cpu.brand().to_string());
        system_info.insert("ram_bytes".to_string(), sys.total_memory().to_string());
        system_info.insert("swap_bytes".to_string(), sys.total_swap().to_string());

        system_info
    }

    /// Attempts to read the rust edition from Cargo.toml in the current directory or CARGO_MANIFEST_DIR
    fn get_rust_edition() -> String {
        // Attempt to read Cargo.toml in the current directory or CARGO_MANIFEST_DIR
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        let cargo_toml_path = format!("{}/Cargo.toml", manifest_dir);

        if let Ok(contents) = fs::read_to_string(cargo_toml_path) {
            for line in contents.lines() {
                if line.trim_start().starts_with("edition = ")
                    && let Some(edition) = line.split('=').nth(1)
                {
                    return edition.trim().trim_matches('"').to_string();
                }
            }
        }

        "<unknown>".to_string()
    }

    /// Inspects the 'strings' portion of the binary to find the rustc version used to compile it.
    fn get_rust_compiler() -> String {
        let executable = executable();

        if let Ok(output) = std::process::Command::new("strings")
            .args(["-a", executable.to_str().unwrap_or("")])
            .output()
            && output.status.success()
        {
            let output_str = String::from_utf8_lossy(&output.stdout);
            for line in output_str.lines() {
                if line.starts_with("rustc version") {
                    return line.to_string();
                }
            }
        }
        "<unknown>".to_string()
    }

    /// Look for cargo manifest in the current directory OR in one specified by CARGO_MANIFEST_DIR environment variable.  
    /// If found, grab depenendencies for packages specified in CHECK_PACKAGES array.
    fn get_package_info() -> HashMap<String, String> {
        let mut package_info = HashMap::new();

        let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        let lock_path = format!("{}/Cargo.lock", manifest_dir);

        if let Ok(contents) = fs::read_to_string(lock_path) {
            let mut lines = contents.lines();
            while let Some(line) = lines.next() {
                if line.trim_start().starts_with("name = ")
                    && let Some(name) = line.split('=').nth(1)
                {
                    let name = name.trim().trim_matches('"').to_string();
                    if !CHECK_PACKAGES.contains(&name.as_str()) {
                        continue;
                    }

                    let mut version = String::new();
                    let mut source = String::new();

                    // Look ahead for version line
                    if let Some(version_line) = lines.next()
                        && version_line.trim_start().starts_with("version = ")
                        && let Some(v) = version_line.split('=').nth(1)
                    {
                        version = v.trim().trim_matches('"').to_string();
                    }

                    // Look ahead for source line
                    if let Some(source_line) = lines.next()
                        && source_line.trim_start().starts_with("source = ")
                        && let Some(s) = source_line.split('=').nth(1)
                    {
                        source = s.trim().trim_matches('"').to_string();
                    }

                    // Store as "version/source" format
                    if !version.is_empty() || !source.is_empty() {
                        let combined = format!("{}/{}", version, source);
                        package_info.insert(name, combined);
                    }
                }
            }
        }

        package_info
    }

    fn get_git_info() -> HashMap<String, String> {
        let mut git_info = HashMap::new();

        // Get long hash
        if let Ok(output) = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            && output.status.success()
        {
            let commit_hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
            git_info.insert("commit_hash".to_string(), commit_hash);
        }

        // Get short hash
        if let Ok(output) = std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            && output.status.success()
        {
            let short_hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
            git_info.insert("short_hash".to_string(), short_hash);
        }

        // Get commit date
        if let Ok(output) = std::process::Command::new("git")
            .args(["log", "-1", "--format=%cd", "--date=iso"])
            .output()
            && output.status.success()
        {
            let commit_date = String::from_utf8_lossy(&output.stdout).trim().to_string();
            git_info.insert("commit_date".to_string(), commit_date);
        }

        // Get commit message
        if let Ok(output) = std::process::Command::new("git")
            .args(["log", "-1", "--format=%s"])
            .output()
            && output.status.success()
        {
            let commit_message = String::from_utf8_lossy(&output.stdout).trim().to_string();
            git_info.insert("commit_message".to_string(), commit_message);
        }

        git_info
    }
}

/// Get the current executable path
fn executable() -> PathBuf {
    env::current_exe().unwrap_or(PathBuf::from("__unknown__"))
}

/// Generate a default benchmark name based on the executable file name
pub fn default_benchmark_name() -> String {
    executable()
        .file_stem()
        .unwrap_or(OsStr::new("__unknown__"))
        .to_string_lossy()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_updates() {
        let mut benchmark_info = BenchmarkInformation::new();
        assert_eq!(benchmark_info.output.len(), 0);

        benchmark_info.with_output("test_key", "test_value".into());
        assert_eq!(benchmark_info.output.len(), 1);
        assert_eq!(benchmark_info.output["test_key"], "test_value");
    }

    #[test]
    fn test_named() {
        let benchmark_info = BenchmarkInformation::with_name("MyBenchmark");
        assert_eq!(benchmark_info.benchmark_name, "MyBenchmark");
    }

    #[test]
    fn test_executable() {
        let exe_path = executable().to_string_lossy().to_string();
        assert!(exe_path.contains("benchmark_record"));
        println!("Executable: {exe_path}");
    }

    #[test]
    fn test_default_output_path() {
        let benchmark_info = BenchmarkInformation::new();
        let output_path = benchmark_info.default_output_path(".");
        let output_path_str = output_path.to_string_lossy().to_string();
        println!("Default output file name: {output_path_str}");

        assert!(output_path_str.ends_with(".json"));
        assert!(output_path_str.contains(default_benchmark_name().as_str()));
    }

    #[test]
    fn test_git_info() {
        let git_info = BenchmarkInformation::get_git_info();

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

    #[test]
    fn test_env_capture() {
        //Must be done in this order since `non_empty` changes things `empty` reads
        test_empty_env();
        test_non_empty_env();
    }

    fn test_empty_env() {
        let env_info = BenchmarkInformation::new();

        println!("Empty environment JSON:");
        env_info.display(Some(2));
        //env_info.display(None);

        assert_eq!(0, env_info.environment_vars.len());
        assert_eq!(0, env_info.slurm_params.len());
    }

    fn test_non_empty_env() {
        unsafe {
            // Simulating setting environment variables on the command line.
            env::set_var("SLURM_1", "1");
            env::set_var("SLURM_2", "2");
            env::set_var("SLURM_3", "3");

            env::set_var("LAMELLAR_A", "A");
            env::set_var("LAMELLAR_B", "B");
        }

        let env_info = BenchmarkInformation::new();

        assert_eq!(3, env_info.slurm_params.len());
        assert_eq!(2, env_info.environment_vars.len());
        assert_eq!(env_info.slurm_params.get("SLURM_1").unwrap(), "1");
        assert_eq!(env_info.slurm_params.get("SLURM_2").unwrap(), "2");
        assert_eq!(env_info.slurm_params.get("SLURM_3").unwrap(), "3");
        assert_eq!(env_info.environment_vars.get("LAMELLAR_A").unwrap(), "A");
        assert_eq!(env_info.environment_vars.get("LAMELLAR_B").unwrap(), "B");
    }
}
