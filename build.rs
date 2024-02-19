use ethers::prelude::Abigen;
use std::{env, fs, path::Path};

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let abi_sources = fs::read_dir("src/ABI").unwrap();

    for abi_source in abi_sources {
        let abi_source = abi_source.unwrap();
        let abi_name = abi_source.file_name();
        let abi_name = Path::new(&abi_name)
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap();
        let abi_out_file = Path::new(&out_dir).join(format!("{}.rs", abi_name));
        if abi_out_file.exists() {
            fs::remove_file(&abi_out_file).unwrap();
        }
        Abigen::new(abi_name, abi_source.path().to_str().unwrap())
            .expect("ABIGEN FAILED")
            .generate()
            .expect("ABIGEN FAILED")
            .write_to_file(abi_out_file)
            .unwrap();
    }
    println!("cargo:rerun-if-changed=build.rs");
}
