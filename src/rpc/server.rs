// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "rdma")]
use ibv::connection::conn::Conn;
#[cfg(feature = "rdma")]
use ibv::connection::conn::MyReceiver;
#[allow(unused_imports)]
use log::{debug, error, info, warn};
#[cfg(feature = "tcp")]
use tokio::net::TcpListener;
#[cfg(feature = "rdma")]
use tokio::sync::mpsc::channel;

use super::{connection::ServerConnection, protocol::RequestHeader};

#[async_trait]
pub trait Handler {
    async fn dispatch(
        &self,
        operation_type: u32,
        flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, Vec<u8>, Vec<u8>)>;
}

pub async fn handle<H: Handler + std::marker::Sync + std::marker::Send + 'static>(
    handler: Arc<H>,
    connection: Arc<ServerConnection>,
    header: RequestHeader,
    path: Vec<u8>,
    data: Vec<u8>,
    metadata: Vec<u8>,
) {
    debug!("handle, id: {}", header.id);
    let response = handler
        .dispatch(header.r#type, header.flags, path, data, metadata)
        .await;
    debug!("handle, response: {:?}", response);
    match response {
        Ok(response) => {
            let result = connection
                .send_response(
                    header.batch,
                    header.id,
                    response.0,
                    response.1,
                    &response.2,
                    &response.3,
                )
                .await;
            match result {
                Ok(_) => {
                    debug!("handle, send response success");
                }
                Err(e) => {
                    debug!("handle, send response error: {}", e);
                }
            }
        }
        Err(e) => {
            debug!("handle, dispatch error: {}", e);
        }
    }
}

pub async fn test() {
    info!("test");
}

// receive(): handle the connection
// 1. read the request header
// 2. read the request data
// 3. spawn a handle thread to handle the request
// 4. loop to 1
pub async fn receive<H: Handler + std::marker::Sync + std::marker::Send + 'static>(
    handler: Arc<H>,
    connection: Arc<ServerConnection>,
) {
    loop {
        #[cfg(feature = "rdma")]
        {
            let request: &[u8] = connection.conn.recv_msg().await.unwrap();
            let (header, path, meta_data, data) = parse_request(request);
            connection.conn.release(request).await;

            let handler = handler.clone();
            tokio::spawn(handle(
                handler,
                connection.clone(),
                header,
                path,
                meta_data,
                data,
            ));
        }
        #[cfg(feature = "tcp")]
        {
            let id = connection.name_id();
            debug!("{:?} parse_request, start", id);
            let header = match connection.receive_request_header().await {
                Ok(header) => header,
                Err(e) => {
                    if e.to_string() == "early eof" {
                        warn!("connection {:?} is closed abnormally.", id);
                    } else {
                        error!("{:?} parse_request, header error: {}", id, e);
                    }
                    break;
                }
            };
            debug!("{:?} parse_request, header: {}", id, header.id);
            let data_result = connection.receive_request(&header).await;
            let (path, data, metadata) = match data_result {
                Ok(data) => data,
                Err(e) => {
                    error!("{:?} parse_request, data error: {}", id, e);
                    break;
                }
            };
            debug!("{:?} parse_request, data: {}", id, header.id);
            let handler = handler.clone();
            let connection = connection.clone();
            tokio::spawn(handle(handler, connection, header, path, data, metadata));
        }
    }
}

pub struct Server<H: Handler + std::marker::Sync + std::marker::Send + 'static> {
    // listener: TcpListener,
    bind_address: String,
    #[cfg(feature = "rdma")]
    incoming: MyReceiver<Conn>,
    handler: Arc<H>,
}

impl<H: Handler + std::marker::Sync + std::marker::Send> Server<H> {
    pub fn new(handler: Arc<H>, bind_address: &str) -> Self {
        #[cfg(feature = "rdma")]
        let incoming = {
            let (tx, rx) = channel(1000);
            tokio::spawn(ibv::connection::conn::run(bind_address.to_string(), tx));
            MyReceiver::new(rx)
        };
        Self {
            handler,
            bind_address: String::from(bind_address),
            #[cfg(feature = "rdma")]
            incoming,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        info!("Listening on {:?}", self.bind_address);
        #[cfg(feature = "tcp")]
        let listener = TcpListener::bind(&self.bind_address).await?;
        let mut id = 1;
        loop {
            #[cfg(feature = "tcp")]
            let stream_or_conn = listener.accept().await.unwrap().0;
            #[cfg(feature = "rdma")]
            let stream_or_conn = self.incoming.recv().await;
            info!("Connection {id} accepted");
            let handler = Arc::clone(&self.handler);
            let name_id = format!("{},{}", self.bind_address, id);
            let connection = Arc::new(ServerConnection::new(stream_or_conn, name_id));
            tokio::spawn(async move {
                receive(handler, connection).await;
            });
            id += 1;
        }
    }
}

#[cfg(feature = "rdma")]
pub fn parse_request_header(request: &[u8]) -> RequestHeader {
    use super::protocol::REQUEST_HEADER_SIZE;

    let header = &request[0..REQUEST_HEADER_SIZE];
    let batch = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let id = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let operation_type = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let flags: u32 = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let total_length = u32::from_le_bytes(header[16..20].try_into().unwrap());
    let file_path_length = u32::from_le_bytes(header[20..24].try_into().unwrap());
    let meta_data_length = u32::from_le_bytes(header[24..28].try_into().unwrap());
    let data_length = u32::from_le_bytes(header[28..32].try_into().unwrap());
    RequestHeader {
        batch,
        id,
        r#type: operation_type,
        flags,
        total_length,
        file_path_length,
        meta_data_length,
        data_length,
    }
}

#[cfg(feature = "rdma")]
pub fn parse_request(request: &[u8]) -> (RequestHeader, Vec<u8>, Vec<u8>, Vec<u8>) {
    use crate::rpc::protocol::REQUEST_HEADER_SIZE;

    let header = parse_request_header(request);
    debug!("parse_request, header: {:?}", header);
    let path =
        &request[REQUEST_HEADER_SIZE..REQUEST_HEADER_SIZE + header.file_path_length as usize];
    let metadata = &request[REQUEST_HEADER_SIZE + header.file_path_length as usize
        ..REQUEST_HEADER_SIZE
            + header.file_path_length as usize
            + header.meta_data_length as usize];
    let data = &request[REQUEST_HEADER_SIZE
        + header.file_path_length as usize
        + header.meta_data_length as usize
        ..REQUEST_HEADER_SIZE
            + header.file_path_length as usize
            + header.meta_data_length as usize
            + header.data_length as usize];
    (header, path.to_vec(), metadata.to_vec(), data.to_vec())
}
