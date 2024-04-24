use serde_json::Value;
use serde_json::json;
use tokio::sync::mpsc;

use crate::message::Message;

pub async fn parse_record(record: Value, tx: mpsc::Sender<Message>) -> Result<(), Box<dyn std::error::Error>> {

    let last_event = record;

    let event_action = last_event
        .get("hits")
        .and_then(|v| v.get("hits"))
        .and_then(|v| v.get(0))
        .and_then(|v| v.get("_source"))
        .and_then(|v| v.get("event"))
        .and_then(|v| v.get("action"))
        .and_then(|v| v.get(0))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let event_type = last_event
        .get("hits")
        .and_then(|v| v.get("hits"))
        .and_then(|v| v.get(0))
        .and_then(|v| v.get("_source"))
        .and_then(|v| v.get("event"))
        .and_then(|v| v.get("type"))
        .and_then(|v| v.get(0))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let file_path = last_event
        .get("hits")
        .and_then(|v| v.get("hits"))
        .and_then(|v| v.get(0))
        .and_then(|v| v.get("_source"))
        .and_then(|v| v.get("file"))
        .and_then(|v| v.get("uri"))
        .and_then(|v| v.as_str())
        .unwrap_or("empty_file_path");

    let record_id_and_index = if event_action == "moved" || event_action == "deleted" {
            ("no_id".to_string(),"no_index".to_string())
        } else {
            
            let record_id = last_event
            .get("hits")
            .and_then(|v| v.get("hits"))
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("empty_record_id");
    
        let record_index = last_event
            .get("hits")
            .and_then(|v| v.get("hits"))
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("_index"))
            .and_then(|v| v.as_str())
            .unwrap_or("empty_record_index");
        
            (record_id.to_string(),record_index.to_string())
        };

    // println!("File Path: {:?}", file_path);
    // println!("Record ID: {:?}", record_id_and_index);

    let payload = json!({
        "event_type": event_type,
        "file_path": file_path,
        "record_id": record_id_and_index.0,
        "record_index": record_id_and_index.1,
    });

    log::debug!("Parsed record: {}", payload);

    let message = Message::Delete {
        event_type: event_type.to_string(),
        payload: payload,
    };

    tx.send(message).await?;

    Ok(())
}