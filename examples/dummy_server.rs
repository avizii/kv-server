use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tracing::info;
use kv_server::{CommandRequest, CommandResponse};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;

    info!("start listening on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("client: {:?} connected", addr);

        tokio::spawn(async move {
            let mut stream = AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();

            while let Some(Ok(msg)) = stream.next().await {
                info!("Got a new command: {:?}", msg);

                let mut resp: CommandResponse = CommandResponse::default();
                resp.status = 404;
                resp.message = "Not Found".to_string();

                stream.send(resp).await.unwrap();
            }
            info!("client {:?} disconnected", addr);
        });
    }
}