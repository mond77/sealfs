use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaBuilder};
use kanal;
use log::debug;
use std::io::Write;
use std::mem;
use std::sync::Arc;
use std::{alloc::Layout, io};

use crate::rpc::protocol::{
    RequestHeader, ResponseHeader, REQUEST_HEADER_SIZE, RESPONSE_HEADER_SIZE,
};

use super::protocol::REQUEST_POOL_SIZE;

pub const RDMA_BUFFER_SIZE: usize = 1024;

pub struct Client {
    rdma: Arc<Rdma>,
    channels: Vec<(kanal::AsyncSender<()>, kanal::AsyncReceiver<()>)>,
    ids: (kanal::AsyncSender<u32>, kanal::AsyncReceiver<u32>),
}

impl Client {
    pub async fn new() -> Self {
        let rdma = RdmaBuilder::default()
            .connect("localhost:5555")
            .await
            .unwrap();
        let mut channels = Vec::with_capacity(REQUEST_POOL_SIZE);
        let ids = kanal::unbounded_async::<u32>();
        for i in 0..REQUEST_POOL_SIZE as u32 {
            channels.push(kanal::unbounded_async());
            ids.0.clone_sync().send(i).unwrap();
        }
        Client {
            rdma: Arc::new(rdma),
            channels,
            ids,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn call_remote(
        &self,
        operation_type: u32,
        req_flags: u32,
        path: &str,
        send_meta_data: &[u8],
        send_data: &[u8],
        status: &mut i32,
        rsp_flags: &mut u32,
        recv_meta_data_length: &mut usize,
        recv_data_length: &mut usize,
        recv_meta_data: &mut [u8],
        recv_data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let id = self.ids.1.clone().recv().await?;
        self.send_request(
            id,
            operation_type,
            req_flags,
            path,
            send_meta_data,
            send_meta_data,
        )
        .await?;
        self.wait_for_response(id).await?;
        Ok(())
    }

    pub async fn wait_for_response(&self, id: u32) -> Result<(), Box<dyn std::error::Error>> {
        self.channels[id as usize].1.clone().recv().await?;
        Ok(())
    }

    pub async fn response(&self, id: u32) -> Result<(), Box<dyn std::error::Error>> {
        self.channels[id as usize].0.clone().send(()).await?;
        self.ids.0.clone().send(id).await?;
        Ok(())
    }

    pub async fn send_data(&self, send_data: &[u8]) -> io::Result<()> {
        // alloc 8 bytes local memory
        let mut lmr = self.rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
        // write data into lmr
        let _num = lmr.as_mut_slice().write(send_data)?;
        // send data in mr to the remote end
        self.rdma.send(&lmr).await?;
        Ok(())
    }

    pub async fn send_request(
        &self,
        id: u32,
        operation_type: u32,
        flags: u32,
        filename: &str,
        meta_data: &[u8],
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let filename_length = filename.len();
        let meta_data_length = meta_data.len();
        let data_length = data.len();
        let total_length = filename_length + meta_data_length + data_length;
        if total_length + REQUEST_HEADER_SIZE > RDMA_BUFFER_SIZE {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                "send data too large",
            )));
        }
        debug!(
            "send_request id: {}, type: {}, flags: {}, total_length: {}, filname_length: {}, meta_data_length, {}, data_length: {}, filename: {:?}, meta_data: {:?}",
            id, operation_type, flags, total_length, filename_length, meta_data_length, data_length, filename, meta_data
        );
        let mut lmr = self
            .rdma
            .alloc_local_mr(Layout::new::<[u8; RDMA_BUFFER_SIZE]>())?;
        {
            let mut request = lmr.as_mut_slice();
            request.write_all(&id.to_le_bytes())?;
            request.write_all(&operation_type.to_le_bytes())?;
            request.write_all(&flags.to_le_bytes())?;
            request.write_all(&(total_length as u32).to_le_bytes())?;
            request.write_all(&(filename_length as u32).to_le_bytes())?;
            request.write_all(&(meta_data_length as u32).to_le_bytes())?;
            request.write_all(&(data_length as u32).to_le_bytes())?;
            request.write_all(filename.as_bytes())?;
            request.write_all(meta_data)?;
            request.write_all(data)?;
        }
        self.rdma.send_with_imm(&lmr, total_length as u32).await?;
        Ok(())
    }

    async fn parse_response_header(&self, recv_data: &[u8]) -> io::Result<ResponseHeader> {
        let header = &recv_data[0..RESPONSE_HEADER_SIZE];
        let id = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let status = i32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let flags = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let total_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let meta_data_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
        let data_length = u32::from_le_bytes([header[20], header[21], header[22], header[23]]);
        debug!(
            "received response_header id: {}, status: {}, flags: {}, total_length: {}, meta_data_length: {}, data_length: {}",
            id, status, flags, total_length, meta_data_length, data_length
        );
        Ok(ResponseHeader {
            id,
            status,
            flags,
            total_length,
            meta_data_length,
            data_length,
        })
    }
}

pub async fn parse_response(client: Arc<Client>) {
    loop {
        if let Ok((lmr, imm)) = client.rdma.receive_with_imm().await {
            let data = *lmr.as_slice();
            let len = imm.unwrap() as usize;
            println!("imm: {:?}", len);
            if let Ok(header) = client.parse_response_header(&data).await {
                //copy metadata and data instead of copying to the designed buffer
                let metadata = data
                    [RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + header.meta_data_length as usize]
                    .to_vec();
                let data = data[RESPONSE_HEADER_SIZE + header.meta_data_length as usize
                    ..RESPONSE_HEADER_SIZE
                        + header.meta_data_length as usize
                        + header.data_length as usize]
                    .to_vec();
                if let Ok(()) = client.response(header.id).await {
                    println!("response success");
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }
}

pub struct Server {
    rdma: Arc<Rdma>,
}

impl Server {
    pub async fn new() -> Self {
        let rdma = RdmaBuilder::default()
            .listen("localhost:5555")
            .await
            .unwrap();
        Server {
            rdma: Arc::new(rdma),
        }
    }

    async fn send_data_with_imm(&self, send_data: &[u8]) -> io::Result<()> {
        // alloc 8 bytes local memory
        let send_len = send_data.len();
        if send_len > RDMA_BUFFER_SIZE {
            return Err(io::Error::new(io::ErrorKind::Other, "send data too large"));
        }
        let mut lmr = self
            .rdma
            .alloc_local_mr(Layout::new::<[u8; RDMA_BUFFER_SIZE]>())?;
        // write data into lmr
        let _num = lmr.as_mut_slice().write(&send_data[..send_len])?;
        // send data and imm to the remote end
        self.rdma.send_with_imm(&lmr, send_len as u32).await?;
        Ok(())
    }

    pub fn parse_request_header(
        &self,
        header: &[u8],
    ) -> Result<RequestHeader, Box<dyn std::error::Error>> {
        let id = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let operation_type = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let flags: u32 = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let total_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let file_path_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
        let meta_data_length = u32::from_le_bytes([header[20], header[21], header[22], header[23]]);
        let data_length = u32::from_le_bytes([header[24], header[25], header[26], header[27]]);
        debug!(
            "received request header: id: {}, type: {}, flags: {}, total_length: {}, file_path_length: {}, meta_data_length: {}, data_length: {}",
            id, operation_type, flags, total_length, file_path_length, meta_data_length, data_length
        );
        Ok(RequestHeader {
            id,
            r#type: operation_type,
            flags,
            total_length,
            file_path_length,
            meta_data_length,
            data_length,
        })
    }

    pub async fn response(
        &self,
        header: &ResponseHeader,
        metadata: &[u8],
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut response = Vec::new();
        let header_slice = unsafe {
            std::slice::from_raw_parts(
                header as *const ResponseHeader as *const u8,
                mem::size_of::<ResponseHeader>(),
            )
        };
        response.extend_from_slice(header_slice);
        response.extend_from_slice(metadata);
        response.extend_from_slice(data);
        //response to client
        self.send_data_with_imm(&response).await?;
        Ok(())
    }
}

pub async fn receive_request(server: Arc<Server>) {
    loop {
        if let Ok((lmr, imm)) = server.rdma.receive_with_imm().await {
            let data = *lmr.as_slice();
            let len = imm.unwrap() as usize;
            println!("imm: {:?}", len);
            if let Ok(req_header) = server.parse_request_header(&data[..REQUEST_HEADER_SIZE]) {
                let file_path = String::from_utf8(
                    data[REQUEST_HEADER_SIZE
                        ..(REQUEST_HEADER_SIZE + req_header.file_path_length as usize)]
                        .to_vec(),
                )
                .unwrap();
                println!("file_path: {:?}", file_path);
                let meta_data = &data[(REQUEST_HEADER_SIZE + req_header.file_path_length as usize)
                    ..(REQUEST_HEADER_SIZE
                        + req_header.file_path_length as usize
                        + req_header.meta_data_length as usize)]
                    .to_vec();
                println!("meta_data: {:?}", meta_data);
                let data = data[(REQUEST_HEADER_SIZE
                    + req_header.file_path_length as usize
                    + req_header.meta_data_length as usize)
                    ..(REQUEST_HEADER_SIZE
                        + req_header.file_path_length as usize
                        + req_header.meta_data_length as usize
                        + req_header.data_length as usize)]
                    .to_vec();
                println!("data: {:?}", data);

                //handle

                let resp_header = ResponseHeader {
                    id: req_header.id,
                    status: 0,
                    flags: req_header.flags,
                    total_length: 0,
                    meta_data_length: 0,
                    data_length: 0,
                };
                if let Ok(()) = server.response(&resp_header, &[], &[]).await {
                    println!("response success");
                } else {
                    break;
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }
}
