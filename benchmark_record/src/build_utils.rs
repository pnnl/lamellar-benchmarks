use std::env;

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
///     capture_my_vars,   // Name of the capture macro
///     [
///         "CARGO_PKG_NAME",
///         "PROFILE",
///     ]
/// );
/// 
/// // Usage in build.rs:
/// record_cargo_env_vars(MY_VARS);
/// 
/// // Usage in main code:
/// let mut bench = BenchmarkInformation::new();
/// capture_my_vars!(bench);
/// ```
macro_rules! define_env_vars {
    ($const_name:ident, $macro_name:ident, [$($var_name:literal),* $(,)?]) => {
        /// A set of environment variable names to record
        pub const $const_name: &[&str] = &[
            $($var_name,)*
        ];
        
        /// Macro to capture the compile-time values of the defined environment variables
        #[macro_export]
        macro_rules! $macro_name {
            ($bench:expr) => {
                $(
                    $bench.with_compile_info(
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
    capture_compile_vars,
    [
        "CARGO_CFG_FEATURE",
        "CARGO_CFG_TARGET_ARCH",
        "CARGO_CFG_TARGET_OS",
        "CARGO_CFG_TARGET_FEATURE",
        "CARGO_CFG_TARGET_HAS_ATOMIC",
        "CARGO_CFG_TARGET_POINTER_WIDTH",
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

// Keep the original with_compile_info_vars macro for backward compatibility
#[macro_export]
macro_rules! with_compile_info_vars {
    ($bench:expr, [$($var_name:literal),* $(,)?]) => {
        $(
            $bench.with_compile_info(
                $var_name,
                option_env!($var_name)
                    .unwrap_or(concat!($var_name, " variable not set"))
                    .to_string()
            );
        )*
    };
}