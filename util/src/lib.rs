use std::env;
use std::path::PathBuf;
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::collections::HashMap;
use json::JsonValue;
use std::time::{SystemTime, UNIX_EPOCH};
use sysinfo;
use chrono;

const CHECK_PACKAGES: [&str; 4] = ["lamellar", "rofi", "rofisys", "lamellar-impl"];

pub struct SystemInformation {
    pub benchark_name: String,
    pub executable: PathBuf,
    pub parameters: Vec<String>,
    pub run_date: String,
    pub output: Option<HashMap<String, String>>,
    pub build_type: String,
    pub package_info: HashMap<String, String>,
    pub git: HashMap<String, String>,
    pub slurm_params: HashMap<String, String>,
    pub system: HashMap<String, String>,
    pub environment_vars: HashMap<String, String>,
    
}

impl SystemInformation  {
    pub fn new() -> Self {
        let executable = executable();
        let benchark_name = default_benchmark_name();

        Self {
            benchark_name: benchark_name,
            executable: executable,
            parameters: env::args().skip(1).collect(),
            run_date: SystemInformation::get_run_date(),
            output: None,
            build_type: SystemInformation::get_build_type(),
            package_info: SystemInformation::get_package_info(),
            git: SystemInformation::get_git_info(),
            slurm_params: SystemInformation::collect_env_vars("SLURM"),
            system: SystemInformation::get_system_info(),
            environment_vars: SystemInformation::collect_env_vars("LAMELLAR"),
        }
    }

    pub fn with_output(self, output: HashMap<String, String>) -> Self {
        Self {
            benchark_name: self.benchark_name,
            executable: self.executable,
            parameters: self.parameters,
            run_date: self.run_date,
            output: Some(output),
            build_type: self.build_type,
            package_info: self.package_info,
            git: self.git,
            slurm_params: self.slurm_params,
            system: self.system,
            environment_vars: self.environment_vars,
        }
    }

    /// Convert the captured information into a JsonValue object.
    /// This is not intended as a stable API, but may be useful for some cases...use with caution.
    pub fn as_json(&self) -> JsonValue {
        json::object! {
            "benchark name" => self.benchark_name.clone(),
            "executable" => self.executable.to_string_lossy().to_string(),
            "parameters" => self.parameters.clone(),
            "run_date" => self.run_date.clone(),
            "output" => self.output.clone().unwrap_or(HashMap::new()),
            "build type" => self.build_type.clone(),
            "dependencies" => self.package_info.clone(),
            "git" => self.git.clone(),
            "system" => self.system.clone(),
            "environment" => self.environment_vars.clone(),
            "slurm_params" => self.slurm_params.clone(),
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
        if let Ok(mut f) = OpenOptions::new().create(true).write(true).append(true).open(file) {
            let _ = writeln!(f, "{}", json::stringify(json_obj));
        }
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
        let build_type = parent.file_name().unwrap_or(&OsStr::new("<unknown>")).to_string_lossy().to_string();
        if ["debug", "release"].contains(&build_type.as_str()) {
            build_type
        } else {
            "<unknown>".to_string()
        }
    }

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
        system_info.insert("physical_cpu_cores".to_string(), sysinfo::System::physical_core_count().unwrap_or(0).to_string());
        let cpu = &sys.cpus()[0];
        system_info.insert("cpu_frequency_mhz".to_string(), cpu.frequency().to_string());
        system_info.insert("cpu_vendor_id".to_string(), cpu.vendor_id().to_string());
        system_info.insert("cpu_brand".to_string(), cpu.brand().to_string());
        system_info.insert("ram_bytes".to_string(), sys.total_memory().to_string());
        system_info.insert("swap_bytes".to_string(), sys.total_swap().to_string());
        
        system_info
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
                if line.trim_start().starts_with("name = ") {
                    if let Some(name) = line.split('=').nth(1) {
                        let name = name.trim().trim_matches('"').to_string();
                        if !CHECK_PACKAGES.contains(&name.as_str()) {
                            continue;
                        }

                        if let Some(version_line) = lines.next() {
                            if version_line.trim_start().starts_with("version = ") {
                                if let Some(version) = version_line.split('=').nth(1) {
                                    let version = version.trim().trim_matches('"').to_string();
                                    package_info.insert(name, version);
                                }
                            }
                        }
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
            .args(&["rev-parse", "HEAD"])
            .output()
        {
            if output.status.success() {
                let commit_hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
                git_info.insert("commit_hash".to_string(), commit_hash);
            }
        }
        
        // Get short hash
        if let Ok(output) = std::process::Command::new("git")
            .args(&["rev-parse", "--short", "HEAD"])
            .output()
        {
            if output.status.success() {
                let short_hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
                git_info.insert("short_hash".to_string(), short_hash);
            }
        }
        
        // Get commit date
        if let Ok(output) = std::process::Command::new("git")
            .args(&["log", "-1", "--format=%cd", "--date=iso"])
            .output()
        {
            if output.status.success() {
                let commit_date = String::from_utf8_lossy(&output.stdout).trim().to_string();
                git_info.insert("commit_date".to_string(), commit_date);
            }
        }
        
        // Get commit message
        if let Ok(output) = std::process::Command::new("git")
            .args(&["log", "-1", "--format=%s"])
            .output()
        {
            if output.status.success() {
                let commit_message = String::from_utf8_lossy(&output.stdout).trim().to_string();
                git_info.insert("commit_message".to_string(), commit_message);
            }
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
    executable().file_stem().unwrap_or(&OsStr::new("__unknown__")).to_string_lossy().to_string()
}


/// Generate a default output file name based on the benchmark name and current timestamp
pub fn default_output_path() -> String {
    let stem  = default_benchmark_name();
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    format!("{stem}-{timestamp}.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executable() {
        let exe_path = executable().to_string_lossy().to_string();
        assert!(exe_path.contains("util"));
        println!("Executable: {exe_path}");
    }

    #[test]
    fn test_default_output_path() {
        let output_path = default_output_path();
        println!("Default output file name: {output_path}");

        assert!(output_path.ends_with(".json"));
        assert!(output_path.contains(default_benchmark_name().as_str()));
    }


    #[test]
    fn test_git_info() {
        let git_info = SystemInformation::get_git_info();
        
        println!("Git info: {:?}", git_info);
        
        // Check that all expected git fields are present
        assert!(git_info.contains_key("commit_hash"), "commit_hash should be present");
        assert!(git_info.contains_key("short_hash"), "short_hash should be present");
        assert!(git_info.contains_key("commit_date"), "commit_date should be present");
        assert!(git_info.contains_key("commit_message"), "commit_message should be present");
        
        // Check that values are not empty (assuming we're in a git repository)
        if let Some(commit_hash) = git_info.get("commit_hash") {
            assert!(!commit_hash.is_empty(), "commit_hash should not be empty");
            assert!(commit_hash.len() >= 40, "commit_hash should be at least 40 characters");
        }
        
        if let Some(short_hash) = git_info.get("short_hash") {
            assert!(!short_hash.is_empty(), "short_hash should not be empty");
            assert!(short_hash.len() >= 7, "short_hash should be at least 7 characters");
        }
        
        if let Some(commit_date) = git_info.get("commit_date") {
            assert!(!commit_date.is_empty(), "commit_date should not be empty");
        }
        
        if let Some(commit_message) = git_info.get("commit_message") {
            assert!(!commit_message.is_empty(), "commit_message should not be empty");
        }
    }

    #[test]
    fn test_env_capture() {
        //Must be done in this order since `non_empty` changes things `empty` reads
        test_empty_env();
        test_non_empty_env();
    }

    fn test_empty_env() {
        let env_info = SystemInformation::new();

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

        let env_info = SystemInformation::new();

        assert_eq!(3, env_info.slurm_params.len());
        assert_eq!(2, env_info.environment_vars.len());
        assert_eq!(env_info.slurm_params.get("SLURM_1").unwrap(), "1");
        assert_eq!(env_info.slurm_params.get("SLURM_2").unwrap(), "2");
        assert_eq!(env_info.slurm_params.get("SLURM_3").unwrap(), "3");
        assert_eq!(env_info.environment_vars.get("LAMELLAR_A").unwrap(), "A");
        assert_eq!(env_info.environment_vars.get("LAMELLAR_B").unwrap(), "B");
    }
}
