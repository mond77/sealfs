//! cargo run --example rdma_server --features disk-db

use sealfs::rpc::rdma::{receive_request, Server};
use std::sync::Arc;

#[tokio::main]
pub async fn main() {
    let server = Arc::new(Server::new().await);
    println!("server get connected");

    receive_request(server).await;

    println!("done");
}
