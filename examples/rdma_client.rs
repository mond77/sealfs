//! cargo run --example rdma_client --features disk-db

use log::debug;
use sealfs::rpc::rdma::{parse_response, Client};
use std::sync::Arc;

#[tokio::main]
pub async fn main() {
    let cli = Arc::new(Client::new().await);
    println!("client start");
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    tokio::spawn(parse_response(cli.clone()));
    let mut handles = vec![];
    for i in 0..3 {
        let new_client = cli.clone();
        handles.push(tokio::spawn(async move {
            let mut status = 0;
            let mut rsp_flags = 0;
            let mut recv_meta_data_length = 0;
            let mut recv_data_length = 0;
            let mut recv_meta_data = vec![];
            let mut recv_data = vec![0u8; 1024];
            debug!("call_remote, start");
            let result = new_client
                .call_remote(
                    0,
                    0,
                    "hello",
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut recv_meta_data,
                    &mut recv_data,
                )
                .await;
            debug!("call_remote, result: {:?}", result);
            match result {
                Ok(_) => {
                    if status == 0 {
                        let data = String::from_utf8(recv_data).unwrap();
                        // println!("result: {}, data: {}", i, data);
                    } else {
                        println!("Error: {}", status);
                    }
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        }));
    }
    for h in handles {
        h.await;
    }
    println!("done");
}
