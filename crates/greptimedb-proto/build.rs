//! Resolves the GreptimeDB protocol sources and compiles them into prost
//! messages plus grpc-build service modules.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use grpc_build::{SchemaKind, render_service_module};

const PROTO_REPOSITORY: &str = "https://github.com/GreptimeTeam/greptime-proto.git";
/// Wire-ABI revision. The generated messages and services must not drift with
/// the repository's default branch.
const PROTO_REVISION: &str = "acb247415b8f4c2f0750783dab4e1f38c71f5860";
const CACHE_DIRECTORY: &str = ".cache/greptime-proto";
const OVERRIDE_VARIABLE: &str = "GREPTIME_PROTO_DIR";

fn main() {
    println!("cargo:rerun-if-env-changed={OVERRIDE_VARIABLE}");
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=HOME");
    let Some(manifest_dir) = env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from) else {
        panic!("CARGO_MANIFEST_DIR is set by cargo");
    };
    let Some(out_dir) = env::var_os("OUT_DIR").map(PathBuf::from) else {
        panic!("OUT_DIR is set by cargo");
    };
    let local_proto_dir = manifest_dir.join("proto");
    let proto_root = resolve_proto_root();

    println!("cargo:rerun-if-changed={}", proto_root.display());
    println!("cargo:rerun-if-changed={}", local_proto_dir.display());

    let includes = [proto_root, local_proto_dir];
    let protos = collect_protos(&includes);
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    compile_messages(&protos, &includes, &out_dir);
    render_services(&protos, &includes, &out_dir);
}

/// Returns the directory that `greptime/v1` is relative to.
fn resolve_proto_root() -> PathBuf {
    if let Some(dir) = env::var_os(OVERRIDE_VARIABLE).map(PathBuf::from) {
        return override_proto_root(&dir);
    }
    let home = env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| {
        panic!("HOME is unset and {OVERRIDE_VARIABLE} was not provided; set {OVERRIDE_VARIABLE} to a greptime-proto checkout")
    });
    let cache = home.join(CACHE_DIRECTORY);
    let cached_root = cache.join("proto");
    if !cached_root.join("greptime").join("v1").is_dir() {
        clone_cache(&cache);
    }
    pin_cache(&cache);
    if !cached_root.join("greptime").join("v1").is_dir() {
        panic!(
            "cloned {PROTO_REPOSITORY} into {} but {} still does not contain greptime/v1",
            cache.display(),
            cached_root.display()
        );
    }
    cached_root
}

/// Moves the cache to the pinned revision, fetching it when the initial shallow
/// clone of the default branch does not contain it.
fn pin_cache(cache: &Path) {
    if git_output(cache, &["rev-parse", "HEAD"]).trim() == PROTO_REVISION {
        return;
    }
    git_output(cache, &["fetch", "--depth", "1", "origin", PROTO_REVISION]);
    git_output(cache, &["checkout", "--detach", "FETCH_HEAD"]);
    let head = git_output(cache, &["rev-parse", "HEAD"]);
    if head.trim() != PROTO_REVISION {
        panic!(
            "greptime-proto cache {} is at {} instead of the pinned revision {PROTO_REVISION}; set {OVERRIDE_VARIABLE} to an explicit checkout or remove the cache",
            cache.display(),
            head.trim()
        );
    }
}

fn git_output(cache: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(cache)
        .args(args)
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to run git -C {} {}: {error}; set {OVERRIDE_VARIABLE} to an existing checkout",
                cache.display(),
                args.join(" "),
            )
        });
    if !output.status.success() {
        panic!(
            "git -C {} {} exited with {}: {}",
            cache.display(),
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn override_proto_root(dir: &Path) -> PathBuf {
    if dir.join("proto").join("greptime").join("v1").is_dir() {
        return dir.join("proto");
    }
    if dir.join("greptime").join("v1").is_dir() {
        return dir.to_path_buf();
    }
    panic!(
        "{OVERRIDE_VARIABLE}={} must be a greptime-proto repository root containing proto/greptime/v1 or the proto directory itself containing greptime/v1",
        dir.display()
    );
}

fn clone_cache(cache: &Path) {
    let status = Command::new("git")
        .args(["clone", "--depth", "1", PROTO_REPOSITORY])
        .arg(cache)
        .status()
        .unwrap_or_else(|error| {
            panic!(
                "failed to run git clone {PROTO_REPOSITORY}: {error}; set {OVERRIDE_VARIABLE} to an existing checkout"
            )
        });
    if !status.success() {
        panic!(
            "git clone {PROTO_REPOSITORY} into {} exited with {status}; set {OVERRIDE_VARIABLE} to an existing checkout or remove the cache directory",
            cache.display()
        );
    }
}

fn collect_protos(includes: &[PathBuf]) -> Vec<PathBuf> {
    let mut protos = Vec::new();
    for root in includes {
        collect_recursive(root, &mut protos);
    }
    protos.sort();
    protos
}

fn collect_recursive(dir: &Path, protos: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|error| {
        panic!("failed to read proto directory {}: {error}", dir.display())
    });
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| {
                panic!("failed to read an entry under {}: {error}", dir.display())
            })
            .path();
        if path.is_dir() {
            collect_recursive(&path, protos);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "proto")
        {
            protos.push(path);
        }
    }
}

fn compile_messages(protos: &[PathBuf], includes: &[PathBuf], out_dir: &Path) {
    let mut config = prost_build::Config::new();
    config.out_dir(out_dir);
    if let Some(protoc) = protoc_path() {
        config.protoc_executable(protoc);
    }
    config
        .compile_protos(protos, includes)
        .unwrap_or_else(|error| panic!("failed to compile the GreptimeDB protobuf set: {error}"));
}

fn protoc_path() -> Option<PathBuf> {
    if let Some(protoc) = env::var_os("PROTOC").map(PathBuf::from) {
        return Some(protoc);
    }
    let bundled = PathBuf::from("/root/.local/bin/protoc");
    bundled.is_file().then_some(bundled)
}

fn render_services(protos: &[PathBuf], includes: &[PathBuf], out_dir: &Path) {
    for proto in protos {
        let source = fs::read_to_string(proto)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", proto.display()));
        if !defines_service(&source) {
            continue;
        }
        let rendered = render_service_module(SchemaKind::Proto, &source).unwrap_or_else(|error| {
            panic!(
                "failed to render services from {}: {error}",
                proto.display()
            )
        });
        let output = out_dir.join(service_file_name(proto, includes));
        fs::write(&output, rendered)
            .unwrap_or_else(|error| panic!("failed to write {}: {error}", output.display()));
    }
}

fn defines_service(source: &str) -> bool {
    source
        .lines()
        .any(|line| line.trim().starts_with("service "))
}

/// Names the rendered service module after the schema's path inside its
/// include root so that names stay unique across packages.
fn service_file_name(proto: &Path, includes: &[PathBuf]) -> String {
    let relative = includes
        .iter()
        .find_map(|root| proto.strip_prefix(root).ok())
        .unwrap_or_else(|| {
            panic!(
                "{} is not inside any configured proto include root",
                proto.display()
            )
        });
    let flattened = relative.to_string_lossy().replace('/', ".");
    let stem = flattened
        .strip_suffix(".proto")
        .unwrap_or_else(|| panic!("{} does not have a .proto extension", proto.display()));
    format!("{stem}_grpc.rs")
}
