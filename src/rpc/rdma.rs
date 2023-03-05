use async_rdma::{LocalMrReadAccess, Rdma, RdmaBuilder, LocalMrWriteAccess};
use std::io::Write;
use std::{alloc::Layout, io};
use std::sync::Arc;

pub struct Client {
    rdma: Arc<Rdma>,
}

impl Client {
    pub async fn new() -> Self {
        let rdma = RdmaBuilder::default()
            .connect("localhost:5555")
            .await
            .unwrap();
        Client {
            rdma: Arc::new(rdma),
        }
    }

    pub async fn run(&self) {
        let send_data = &[1_u8; 8];
        println!("send data: {:?}", send_data);
        self.send_data(send_data).await;
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

    pub async fn run(&self) {
        let recv_data= &mut [0u8; 8];
        self.receive_data(recv_data).await;
        println!("receive data: {:?}", recv_data);
    }

    async fn receive_data(&self, recv_data: &mut [u8]) -> io::Result<()>{
        // receive data
        let lmr = self.rdma.receive().await?;
        let data = *lmr.as_slice();
        recv_data.copy_from_slice(data);
        assert_eq!(recv_data, [1_u8; 8]);
        Ok(())
    }
}