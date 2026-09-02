use std::net::SocketAddr;

use clap::{Parser, Subcommand};
use runnel_client::Client;
use runnel_protocol::{BinaryPayload, Request, Response};

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
        payload: Option<String>,
        #[arg(long, conflicts_with = "payload")]
        payload_base64: Option<String>,
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
    Replay {
        stream: String,
        consumer: String,
        offset: u64,
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
            payload_base64,
            key,
            request_id,
        } => match (payload, payload_base64) {
            (Some(payload), None) => Request::Publish {
                stream,
                key,
                payload,
                request_id,
            },
            (None, Some(payload_base64)) => {
                let payload = BinaryPayload::from_base64(&payload_base64)
                    .map_err(|error| format!("--payload-base64 is invalid: {error}"))?;
                Request::PublishBytes {
                    stream,
                    key,
                    payload_base64: payload,
                    request_id,
                }
            }
            (None, None) => {
                return Err("publish requires a text payload argument or --payload-base64".into());
            }
            (Some(_), Some(_)) => unreachable!("clap rejects conflicting payload arguments"),
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
        Command::Replay {
            stream,
            consumer,
            offset,
        } => Request::Replay {
            stream,
            consumer,
            offset,
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
