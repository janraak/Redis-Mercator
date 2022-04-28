extern crate bindgen;
 
use std::env;
use std::path::PathBuf;
use std::fs::copy;
 

fn build_legacy(folder: String, file: String, with_bindings: bool) {
    let mut build = cc::Build::new();
    build
    .file(format!("./{}{}.c", folder, file))
    .include("./c-project/")
    .include("../src/")
    .include("./src/")
    .include("../deps/lua/src")
    .include("../deps/hiredis/")
    .compile(&file);
 
    if with_bindings {
        let bindings = bindgen::Builder::default()
            .header(format!("src/{}_xfc.h", file))
            .parse_callbacks(Box::new(bindgen::CargoCallbacks))
            .generate()
            .expect("Unable to generate bindings");
 
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        bindings
            .write_to_file(out_path.join(format!("bindings_{}.rs",file)))
            .expect("Couldn't write bindings!");
    }
}
 
fn main() {
    let in_path = PathBuf::from(env::var("PWD").unwrap());
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let _n = copy(in_path.join("../deps/hiredis/libhiredis.a"), out_path.join("libhiredis.a"));
 
    build_legacy("c-project/".to_string(), "redis_cmd_interceptors".to_string(), true);
 
    println!("cargo:rustc-link-lib=redis_cmd_interceptors");
    println!("cargo:rerun-if-changed=./c-project/redis_cmd_interceptors.c");
    println!("cargo:rerun-if-changed=./c-project/redis_cmd_interceptors.h");
    println!("cargo:rerun-if-changed=src/redis_cmd_interceptors_xfc.h"); 
}
 