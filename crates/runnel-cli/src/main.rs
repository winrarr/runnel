use std::net::SocketAddr;

use clap::{Parser, Subcommand};
use runnel_client::Client;
use runnel_protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "runnelctl", about = "Development client for Runnel")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:4222", global = true)]
    server: SocketAddr,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    CreateStream {
        stream: String,
    },
    Publish {
        stream: String,
        payload: String,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        request_id: Option<String>,
    },
    Consume {
        stream: String,
        consumer: String,
        #[arg(long)]
        member: Option<String>,
    },
    Ack {
        stream: String,
        consumer: String,
        offset: u64,
        #[arg(long)]
        member: Option<String>,
        #[arg(long)]
        delivery_token: Option<String>,
    },
    Health,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let request = match args.command {
        Command::CreateStream { stream } => Request::CreateStream { stream },
        Command::Publish {
            stream,
            payload,
            key,
            request_id,
        } => Request::Publish {
            stream,
            key,
            payload,
            request_id,
        },
        Command::Consume {
            stream,
            consumer,
            member,
        } => match member {
            Some(member) => Request::PollGroup {
                stream,
                consumer,
                member,
            },
            None => Request::Poll { stream, consumer },
        },
        Command::Ack {
            stream,
            consumer,
            offset,
            member,
            delivery_token,
        } => match (member, delivery_token) {
            (Some(member), Some(delivery_token)) => Request::AckGroup {
                stream,
                consumer,
                member,
                offset,
                delivery_token,
            },
            (None, None) => Request::Ack {
                stream,
                consumer,
                offset,
            },
            _ => return Err("--member and --delivery-token must be provided together".into()),
        },
        Command::Health => Request::Health,
    };

    let mut client = Client::connect(args.server).await?;
    let response: Response = client.request(&request).await?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}
