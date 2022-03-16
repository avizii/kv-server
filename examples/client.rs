use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tracing::info;
use kv_server::{CommandRequest, CommandResponse, Kvpair};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";

    let stream = TcpStream::connect(addr).await?;

    let mut client = AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
    client.send(cmd).await?;

    if let Some(Ok(data)) = client.next().await {
        info!("Got Response {:?}", data);
    }

    let cmd = CommandRequest::new_hget("t1", "k1");
    client.send(cmd).await?;

    if let Some(Ok(data)) = client.next().await {
        info!("Got Response {:?}", data);
    }

    let pairs = vec![
        Kvpair::new("k1", 1.into()),
        Kvpair::new("k2", 2.into()),
        Kvpair::new("k3", 3.into()),
    ];

    let cmd = CommandRequest::new_hmset("t1", pairs);
    client.send(cmd).await?;

    if let Some(Ok(data)) = client.next().await {
        info!("Got Response {:?}", data);
    }

    Ok(())
}