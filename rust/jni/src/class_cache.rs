//! Cached JNI class references as GlobalRefs for classloader compatibility.
//!
//! In environments like Spring Boot, classes loaded via `BOOT-INF/lib/` are not
//! visible to the system classloader. When Tokio daemon threads (attached via
//! `attach_current_thread_as_daemon`) call `FindClass`, they resolve through the
//! system classloader, which causes `ClassNotFoundException`.
//!
//! This module caches all class references as `GlobalRef`s during `JNI_OnLoad`
//! (which runs on a Java thread with the correct classloader), then reuses them
//! from async/daemon threads.

use jni::objects::{GlobalRef, JClass, JObject};
use jni::JNIEnv;
use std::sync::OnceLock;

/// Cached JNI class references, populated during `JNI_OnLoad`.
pub struct CachedClasses {
    pub zerobus_exception_class: GlobalRef,
    pub non_retriable_exception_class: GlobalRef,
    pub long_class: GlobalRef,
    pub completable_future_class: GlobalRef,
    pub array_list_class: GlobalRef,
    pub encoded_batch_class: GlobalRef,
}

static CLASS_CACHE: OnceLock<CachedClasses> = OnceLock::new();

/// Initialize the class cache. Must be called during `JNI_OnLoad` while the
/// correct classloader is active.
pub fn init_class_cache(env: &mut JNIEnv) -> Result<(), String> {
    let cache = CachedClasses {
        zerobus_exception_class: find_and_cache(env, "com/databricks/zerobus/ZerobusException")?,
        non_retriable_exception_class: find_and_cache(
            env,
            "com/databricks/zerobus/NonRetriableException",
        )?,
        long_class: find_and_cache(env, "java/lang/Long")?,
        completable_future_class: find_and_cache(env, "java/util/concurrent/CompletableFuture")?,
        array_list_class: find_and_cache(env, "java/util/ArrayList")?,
        encoded_batch_class: find_and_cache(env, "com/databricks/zerobus/EncodedBatch")?,
    };

    CLASS_CACHE
        .set(cache)
        .map_err(|_| "Class cache already initialized".to_string())
}

/// Get the cached class references.
///
/// # Panics
///
/// Panics if the cache has not been initialized (JNI_OnLoad not called).
pub fn get_class_cache() -> &'static CachedClasses {
    CLASS_CACHE
        .get()
        .expect("Class cache not initialized - JNI_OnLoad must be called first")
}

/// Convert a `GlobalRef` (known to wrap a `java.lang.Class`) to a `JClass`.
///
/// # Safety
///
/// This is safe because:
/// - The `GlobalRef` was created from `env.find_class()` in `init_class_cache`
/// - The `GlobalRef` prevents garbage collection of the underlying object
/// - The raw pointer remains valid for the lifetime of the `GlobalRef`
pub fn as_jclass<'local>(global: &GlobalRef) -> JClass<'local> {
    unsafe { JClass::from(JObject::from_raw(global.as_obj().as_raw())) }
}

fn find_and_cache(env: &mut JNIEnv, class_name: &str) -> Result<GlobalRef, String> {
    let class = env
        .find_class(class_name)
        .map_err(|e| format!("Failed to find class {}: {}", class_name, e))?;
    env.new_global_ref(class)
        .map_err(|e| format!("Failed to create global ref for {}: {}", class_name, e))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    /// Files that are allowed to call find_class / throw_new (the cache itself and test code).
    const LINT_EXCLUDED: &[&str] = &["class_cache.rs", "test_helper.rs"];

    /// Any new `find_class` call must go through the cache. If you're adding a new
    /// class lookup, add it to `CachedClasses` in class_cache.rs and use
    /// `as_jclass(&get_class_cache().your_field)` instead of `env.find_class(...)`.
    #[test]
    fn no_find_class_outside_cache() {
        let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut violations = Vec::new();

        for entry in fs::read_dir(&src_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().map_or(true, |ext| ext != "rs") {
                continue;
            }
            let file_name = path.file_name().unwrap().to_str().unwrap();
            if LINT_EXCLUDED.contains(&file_name) {
                continue;
            }

            let content = fs::read_to_string(&path).unwrap();
            for (i, line) in content.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with("//") || trimmed.starts_with('*') {
                    continue;
                }
                if trimmed.contains(".find_class(") {
                    violations.push(format!("  {}:{}: {}", file_name, i + 1, trimmed));
                }
            }
        }

        assert!(
            violations.is_empty(),
            "\nFound find_class() outside class_cache.rs / test_helper.rs.\n\
             Add the class to CachedClasses and use as_jclass(&get_class_cache().field) instead.\n\n{}\n",
            violations.join("\n")
        );
    }

    /// `throw_new` bypasses the cache (it calls find_class internally).
    /// Use `create_exception(env, &cached_ref, msg)` + `env.throw(exc)` instead.
    #[test]
    fn no_throw_new() {
        let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut violations = Vec::new();

        for entry in fs::read_dir(&src_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().map_or(true, |ext| ext != "rs") {
                continue;
            }
            let file_name = path.file_name().unwrap().to_str().unwrap();
            if LINT_EXCLUDED.contains(&file_name) {
                continue;
            }

            let content = fs::read_to_string(&path).unwrap();
            for (i, line) in content.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with("//") || trimmed.starts_with('*') {
                    continue;
                }
                if trimmed.contains(".throw_new(") {
                    violations.push(format!("  {}:{}: {}", file_name, i + 1, trimmed));
                }
            }
        }

        assert!(
            violations.is_empty(),
            "\nFound throw_new() which bypasses the class cache.\n\
             Use create_exception(env, &cached_ref, msg) + env.throw(exc) instead.\n\n{}\n",
            violations.join("\n")
        );
    }
}
