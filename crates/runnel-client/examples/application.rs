use std::{env, error::Error, io, net::SocketAddr};

use runnel_client::{AttemptOutcome, Client, PublishOptions};

const STREAM: &str = "orders";
const CONSUMER: &str = "order-worker";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let broker_address = env::var("RUNNEL_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:4222".to_owned())
        .parse::<SocketAddr>()?;
    let order_id = env::var("ORDER_ID").unwrap_or_else(|_| "order-42".to_owned());
    if order_id.is_empty() {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "ORDER_ID must not be empty").into(),
        );
    }

    let mut payload = format!("order={order_id};binary=").into_bytes();
    payload.extend([0, 0xff, b'\n']);
    let request_id = format!("order-created:{order_id}");
    let publish_options = PublishOptions::default().with_request_id(request_id.clone());

    // Keep one persistent client for the normal application flow.
    let mut client = Client::connect(broker_address).await?;
    client
        .create_stream(STREAM)
        .await
        .map_err(|outcome| outcome_error("create stream", outcome))?;

    let receipt = match client
        .publish_bytes_with_options(STREAM, payload.clone(), publish_options.clone())
        .await
    {
        Ok(receipt) => receipt,
        Err(AttemptOutcome::Unknown(failure)) => {
            eprintln!(
                "publish outcome is unknown ({failure:?}); reconnecting and retrying once with request_id {request_id:?}"
            );
            client.reconnect(broker_address).await?;
            client
                .publish_bytes_with_options(STREAM, payload.clone(), publish_options)
                .await
                .map_err(|outcome| outcome_error("publish retry", outcome))?
        }
        Err(outcome) => return Err(outcome_error("publish", outcome).into()),
    };
    println!(
        "publish confirmed: stream={}, offset={}, request_id={request_id}",
        receipt.stream, receipt.offset
    );

    let Some(message) = client
        .poll_bytes(STREAM, CONSUMER)
        .await
        .map_err(|outcome| outcome_error("poll", outcome))?
    else {
        println!("no unacknowledged order is available for {CONSUMER}");
        return Ok(());
    };

    // Application processing receives the original bytes, including 0xff and NUL.
    println!(
        "processed stream={}, offset={}, payload={:02x?}",
        message.stream, message.offset, message.payload
    );
    let acknowledgement = client
        .ack(STREAM, CONSUMER, message.offset)
        .await
        .map_err(|outcome| outcome_error("acknowledge", outcome))?;
    println!(
        "acknowledged stream={}, consumer={}, offset={}, already_acknowledged={}",
        acknowledgement.stream,
        acknowledgement.consumer,
        acknowledgement.offset,
        acknowledgement.already_acknowledged
    );

    Ok(())
}

fn outcome_error(operation: &str, outcome: AttemptOutcome) -> io::Error {
    let classification = match &outcome {
        AttemptOutcome::Confirmed(_) => "unexpected confirmed",
        AttemptOutcome::Rejected(_) => "rejected",
        AttemptOutcome::Retryable(_) => "retryable",
        AttemptOutcome::Unknown(_) => "unknown; the broker may have processed the request",
    };
    io::Error::other(format!(
        "{operation} had {classification} outcome: {outcome:?}"
    ))
}
