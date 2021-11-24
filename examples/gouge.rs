use std::io::Write;
use tokio::io::{AsyncBufReadExt, BufReader};

pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::Query;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = tokio::io::stdin();
    let rdr = BufReader::new(stdin);
    let mut lines = rdr.lines();
    print!("gouge=# ");
    std::io::stdout().flush().unwrap();
    while let Some(line) = lines.next_line().await? {
        let addr = "http://127.0.0.1:50001";
        let mut client = RpcClient::connect(addr).await?;
        let query = tonic::Request::new(Query {
            sql: line.to_string(),
        });
        let response = client.execute(query).await?;
        let response = response.into_inner();
        for row in response.rows {
            println!("{:?}", row.values);
        }
        print!("gouge=# ");
        std::io::stdout().flush().unwrap();
    }
    Ok(())
}
