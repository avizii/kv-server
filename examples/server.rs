use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tracing::info;
use kv_server::{CommandRequest, CommandResponse, MemTable, Service};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let service: Service = Service::new(MemTable::new());
    
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;

    info!("start listening on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("client: {:?} connected", addr);

        let handler = service.clone();
        
        tokio::spawn(async move {
            let mut stream = AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();

            while let Some(Ok(msg)) = stream.next().await {
                let resp = handler.execute(msg);
                stream.send(resp).await.unwrap();
            }
            info!("client {:?} disconnected", addr);
        });
    }
}