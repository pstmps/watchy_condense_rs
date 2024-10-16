use elasticsearch::SearchParts;
use serde_json::{json, Value};
use tokio::sync::mpsc;
// use tracing::field;

use crate::elastic::create_client;
use crate::elastic::Host;
use crate::message::Message;

// TODO use json! macro to create the query

pub async fn get_last_event_for_record(
    es_host: Host,
    index: &str,
    record: &str,
    tx: mpsc::Sender<Message>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = create_client(es_host)?;

    let page_size = 1; // only get the last event

    let fields = vec![
        "file.type",
        "file.uri",
        "@timestamp",
        "event.type",
        "event.action",
    ];

    let query = json!({
        "size": page_size,
        "_source": fields,
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": {
            "bool": {
              "must": [
                {"term":
                { "file.uri" : record }}
                ]
            }
          }
    })
    .to_string();

    log::debug!("Query: {}", query);

    let value: serde_json::Value = serde_json::from_str(&query)?;

    let response = client
        .search(SearchParts::Index(&[index]))
        .body(value)
        .send()
        .await?;

    log::debug!("Response from ES: {:?}", response);

    let response_body = match response.json::<Value>().await {
        Ok(body) => body,
        Err(_) => return Err("Failed to get last event for record".into()),
    };

    let message = Message::LastRecord {
        event_type: "last_record".to_string(),
        payload: response_body.clone(),
    };

    tx.send(message).await?;

    Ok(())
}
