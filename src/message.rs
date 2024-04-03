use serde_json::Value;

pub enum Message {
    Aggregate{
        event_type: String,
        payload: Value,
    },
    LastRecord{
        event_type: String,
        payload: Value,
    },
    Delete{
        event_type: String,
        payload: Value,
    }
}