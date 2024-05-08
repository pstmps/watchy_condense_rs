use elasticsearch::SearchParts;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use crate::elastic::create_client;
use crate::elastic::Host;
use crate::message::Message;

// TODO use json! macro to create the query

pub async fn get_aggs_entries_from_index(
    es_host: Host,
    index: &str,
    page_size: usize,
    agg_sleep: u64,
    tx: mpsc::Sender<Message>,
) -> Result<(), color_eyre::Report> {
    loop {
        let client = create_client(es_host.clone())?;

        let mut after = String::new();

        let mut hits = 1;

        while hits > 0 {
            hits = 0;

            // let json_query = format!(
            //     r#"{{
            //             "size": 0,
            //             "aggs": {{
            //                 "unique_event_types": {{
            //                     "composite": {{
            //                         "size": {},
            //                         "sources": [
            //                             {{
            //                                 "file": {{
            //                                     "terms": {{
            //                                         "field": "file.uri"
            //                                     }}
            //                                 }}
            //                             }}
            //                         ]
            //                         {}
            //                     }}
            //                 }}
            //             }}
            //     }}"#,
            //         page_size,
            //         after
            //     );

            let json_query = generate_query(page_size, &after)?;

            let value: serde_json::Value = serde_json::from_str(&json_query)?;

            let response = client
                .search(SearchParts::Index(&[index]))
                .body(value)
                .send()
                .await?;

            log::debug!("Response from ES: {:?}", response);

            let response_body = match response.json::<Value>().await {
                Ok(body) => body,
                Err(_) => continue,
            };

            let aggs =
                match response_body["aggregations"]["unique_event_types"]["buckets"].as_array() {
                    Some(aggs) => aggs,
                    None => continue,
                };

            for agg in aggs {
                // let doc_count = agg["doc_count"].as_u64().unwrap();
                let doc_count = match agg["doc_count"].as_u64() {
                    Some(value) => value,
                    None => {
                        log::warn!("doc_count is not a u64 or does not exist");
                        0
                    }
                };

                if doc_count > 1 {
                    let agg_clone = agg.clone();
                    let _tx = tx.clone();
                    tokio::spawn(async move {
                        let message = Message::Aggregate {
                            event_type: "Aggregate".to_string(),
                            payload: agg_clone,
                        };

                        log::debug!("Sending message: {:?}", &message);

                        // _tx.send(message).await.unwrap();
                        if let Err(e) = _tx.send(message).await {
                            log::error!("Failed to send message: {}", e);
                        }
                    });
                } // if doc_count > 1
                hits += 1;
            }

            if hits == 0 {
                break;
            }

            after = response_body["aggregations"]["unique_event_types"]["after_key"]["file"]
                .clone()
                .to_string();

            // after = format!(
            //     r#"
            //         ,
            //         "after": {{
            //             "file": {}
            //         }}

            //         "#,
            //     serde_json::to_string(&after_key).unwrap()
            // );
        }

        log::info!("Aggs task sleeping for {} seconds", agg_sleep);
        //sleep for $agg_sleep seconds
        sleep(Duration::from_secs(agg_sleep)).await;
        
    }
}

fn generate_query(page_size: usize, after: &str) -> Result<String, color_eyre::Report> {
    let sources = json!([
        {
            "file": {
                "terms": {
                    "field": "file.uri"
                }
            }
        }
    ]);

    let mut composite = json!({
        "size": page_size,
        "sources": sources
    });

    // if !after.is_empty() {
    //     let after_value: Value = serde_json::from_str(&after).unwrap();
    //     composite["after"] = after_value;
    // }
    if !after.is_empty() {
        if let Ok(after_value) = serde_json::from_str::<Value>(after) {
            composite["after"] = after_value;
        } else {
            log::error!("Failed to parse after as JSON");
        }
    }

    let query = json!({
        "size": 0,
        "aggs": {
            "unique_event_types": {
                "composite": composite
            }
        }
    })
    .to_string();

    log::debug!("Query: {}", query);

    Ok(query)
}
