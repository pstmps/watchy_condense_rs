use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use elasticsearch::SearchParts;
use serde_json::Value;

use crate::elastic::create_client;
use crate::message::Message;


// TODO use json! macro to create the query

pub async fn get_aggs_entries_from_index(index: &str, page_size: usize, timeout: u64, tx: mpsc::Sender<Message>) -> Result<(), Box<dyn std::error::Error>> {

    loop {
        let client = create_client()?;


        let mut after = String::new();

        let mut hits = 1;

        while hits > 0 {

            hits = 0;
            
            let json_query = format!(
                r#"{{
                        "size": 0,
                        "aggs": {{
                            "unique_event_types": {{
                                "composite": {{
                                    "size": {},
                                    "sources": [
                                        {{
                                            "file": {{
                                                "terms": {{
                                                    "field": "file.path"
                                                }}
                                            }}
                                        }}
                                    ]
                                    {}
                                }}
                            }}
                        }}
                }}"#,
                    page_size,
                    after
                );

            let value: serde_json::Value = serde_json::from_str(&json_query)?;
        
            let response = client
                .search(SearchParts::Index(&[index]))
                .body(value)
                .send()
                .await?;
        
            let response_body = match response.json::<Value>().await {
                Ok(body) => body,
                Err(_) => continue,
            };

            let aggs = match response_body["aggregations"]["unique_event_types"]["buckets"].as_array() {
                Some(aggs) => aggs,
                None => continue,
            };

            for agg in aggs {
                //println!("{:?}", agg);

                let doc_count = agg["doc_count"].as_u64().unwrap();

                if doc_count > 1 {

                    let agg_clone = agg.clone();
                    let _tx = tx.clone();
                    tokio::spawn(async move {

                        let message = Message::Aggregate{
                            event_type: "Aggregate".to_string(),
                            payload: agg_clone};
                        //sleep(Duration::from_secs(4)).await; 
                        _tx.send(message).await.unwrap();
                        //print!(".");

                    });
                    
                }   // if doc_count > 1
                hits += 1;

                
                
            }

            if hits == 0 {
                break;
            }

            let after_key = response_body["aggregations"]["unique_event_types"]["after_key"]["file"].clone();

            after = format!(
                r#"
                    ,
                    "after": {{
                        "file": {}
                    }}
            
                    "#,
                serde_json::to_string(&after_key).unwrap()
            );
        }

            //sleep for $timeout seconds
        sleep(Duration::from_secs(timeout)).await;

    }



    
   // Ok(())
}