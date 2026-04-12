fn main() {
    #[cfg(feature = "napi_support")]
    napi_build::setup();
}
