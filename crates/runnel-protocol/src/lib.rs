use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    CreateStream {
        stream: String,
    },
    Publish {
        stream: String,
        key: Option<String>,
        payload: String,
        #[serde(default)]
        request_id: Option<String>,
    },
    Poll {
        stream: String,
        consumer: String,
    },
    PollGroup {
        stream: String,
        consumer: String,
        member: String,
    },
    Ack {
        stream: String,
        consumer: String,
        offset: u64,
    },
    AckGroup {
        stream: String,
        consumer: String,
        member: String,
        offset: u64,
        delivery_token: String,
    },
    Health,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    StreamCreated {
        stream: String,
        created: bool,
    },
    Published {
        stream: String,
        offset: u64,
    },
    Message {
        stream: String,
        consumer: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<String>,
        offset: u64,
        key: Option<String>,
        payload: String,
        published_at_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_attempt: Option<u32>,
    },
    Empty {
        stream: String,
        consumer: String,
    },
    Acknowledged {
        stream: String,
        consumer: String,
        offset: u64,
        already_acknowledged: bool,
    },
    Health {
        status: String,
        streams: usize,
        storage_bytes: u64,
    },
    Error {
        code: String,
        message: String,
    },
}
