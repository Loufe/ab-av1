#![allow(unused_crate_dependencies)]

#[tokio::main(flavor = "current_thread")]
async fn main() {
    ab_av1::run_cli().await;
}
