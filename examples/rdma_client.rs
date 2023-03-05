//! cargo run --example rdma_client --features disk-db

use sealfs::rpc::rdma::Client;
use std::sync::Arc;

#[tokio::main]
pub async fn main() {
    let cli = Arc::new(Client::new().await);
    println!("client start");
    
    cli.run().await;
    
    println!("done");
}