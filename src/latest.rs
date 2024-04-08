use serde_json::Value;
use tokio::sync::mpsc;
use elasticsearch::SearchParts;


use crate::elastic::create_client;
use crate::message::Message;

// TODO use json! macro to create the query

pub async fn get_last_event_for_record(index: &str, record: &str, tx: mpsc::Sender<Message>) -> Result<(), Box<dyn std::error::Error>> {

    let client = create_client()?;

    let page_size = 1; // only get the last event

    let fields = vec!["file.type", "file.path", "@timestamp", "event.type", "event.action"];
    let fields: Vec<Value> = fields.into_iter().map(|s| Value::String(s.to_string())).collect();
    let fields = serde_json::to_string(&fields)?;

    let query = format!(
        r#"{{
                "size": "{}",
                "_source": {},
                "sort": [
                    {{"@timestamp": {{"order": "desc"}}}}
                ],
                "query": {{
                    "bool": {{
                      "must": [
                        {{"term": 
                        {{ "file.path" : "{}" }}}}
                        ]
                    }}
                  }}
        }}"#,
            page_size,
            fields,
            record
        );
        

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
