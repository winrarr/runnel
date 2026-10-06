fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build scripts execute before crate compilation and do not share
    // process environment with application code.
    unsafe { std::env::set_var("PROTOC", protoc) };
    prost_build::compile_protos(&["proto/runnel.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/runnel.proto");
    Ok(())
}
