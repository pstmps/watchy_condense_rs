use elasticsearch::http::transport::TransportBuilder;
use elasticsearch::DeleteByQueryParts;
#[allow(unused_imports)]
use elasticsearch::{http::transport::Transport, OpenPointInTimeParts, Elasticsearch, Error as EsError, SearchParts};
use serde_json::{value, Value};
use serde_json::json;
use std::error::Error;
use std::f32::consts::E;
use std::hash::Hash;

use std::collections::{HashMap, HashSet};
// use reqwest::Client;
use url::Url;

use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use std::sync::{Arc, Mutex};

// ES_USER = "elastic"
// ES_PASSWORD = "f9qXA9bfmLCqxopJDquh"

// ES_HOSTS = [
//     {   "HOST_IP": "192.168.2.225", 
//         "HOST_PORT": 9200,
//         "HOST_SCHEME": "https",
//         "VERIFY_CERTS": False,
//         "CA_CERTS": None,
//         "SSL_SHOW_WARN": False,
//     }
// ]
#[allow(dead_code)]
struct Host {
    host_ip: &'static str,
    host_port: u16,
    host_scheme: &'static str,
    verify_certs: bool,
    ca_certs: Option<&'static str>,
    ssl_show_warn: bool,
}

fn create_transport() -> Result<Transport, Box<dyn Error>> {
    let es_user = "elastic";
    let es_password = "f9qXA9bfmLCqxopJDquh";

    let es_hosts = vec![
        Host {
            host_ip: "192.168.2.225",
            host_port: 9200,
            host_scheme: "https",
            verify_certs: false,
            ca_certs: None,
            ssl_show_warn: false,
        },
    ];

    let url = Url::parse(&format!(
        "{}://{}:{}/",
        es_hosts[0].host_scheme, es_hosts[0].host_ip, es_hosts[0].host_port
    ))?;

    let connection_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);

    let credentials = elasticsearch::auth::Credentials::Basic(es_user.to_string(), es_password.to_string());

    let transport = TransportBuilder::new(connection_pool)
        .auth(credentials)
        .cert_validation(elasticsearch::cert::CertificateValidation::None)
        .build()?;

    Ok(transport)
    
}

async fn get_last_event_for_record(client: &Elasticsearch, index: &str, record: &str) -> Result<(Value), Box<dyn std::error::Error>> {

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

    let response_body = match response.json::<Value>().await {
        Ok(body) => body,
        Err(_) => return Err("Failed to get last event for record".into()),
    };

    //println!("{:?}", response_body);//["hits"]["hits"][0]["_source"]["file"]["path"]);

    Ok((response_body))

}

async fn get_aggs_entries_from_index(client: Elasticsearch, index: &str, page_size: i32,sender: mpsc::Sender<serde_json::Value>) -> Result<(), Box<dyn std::error::Error>> {

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
            
            let agg_clone = agg.clone();
            let sender_clone = sender.clone();
            tokio::spawn(async move {
                //sleep(Duration::from_secs(4)).await; 
                sender_clone.send(agg_clone).await.unwrap();
                //print!(".");

            });
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

    Ok(())
}

async fn handle_record(index: &str, value: Value) -> Result<(String,(String,String),String), Box<dyn std::error::Error>> {
    //println!("Handling records");

    let client = Elasticsearch::new(create_transport()?);
    let record = value.get("key").unwrap().get("file").unwrap().as_str().unwrap();

    let last_event = match get_last_event_for_record(&client, index, record).await {
        Ok(event) => {
            //println!("Successfully got last event for record");
            event
        },
        Err(e) => {
            println!("Failed to get last event for record: {:?}", e);
            return Err("Failed to get last event for record".into());
        },
    };


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
        .and_then(|v| v.get("path"))
        .and_then(|v| v.as_str())
        .unwrap_or("empty_file_path");

    println!("Event action: {:?}", event_action);


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
        
            ((record_id.to_string(),record_index.to_string()))
        };

    println!("File Path: {:?}", file_path);
    println!("Record ID: {:?}", record_id_and_index);

    Ok((file_path.to_string(), record_id_and_index, event_type.to_string()))
}


async fn delete_records(index: &str, file_paths: HashSet<String>, record_ids: HashSet<(String,String)>) -> Result<(), Box<dyn std::error::Error>> {

    let client = Elasticsearch::new(create_transport()?);

    let mut file_paths_query = vec![];
    let mut wild_card_query = vec![];

    for file_path in &file_paths {
        file_paths_query.push(json!({
            "term": {
                "file.path": file_path
            }
        }));

        wild_card_query.push(json!({
            "wildcard": {
                "file.path": {
                    "value": format!("{}/*", file_path)
                }
            }
        }));


    }

    let mut record_ids_query = vec![];
    for record_id in &record_ids {
        record_ids_query.push(json!({
            "bool": {
                "must": [
                    {"term": {
                        "_id": record_id.0
                    }},
                    {"term": {
                        "_index": record_id.1
                    }}
                ]
            }

        }));
    }

    let query = json!({
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "should": file_paths_query
                        }
                    },
                    {
                        "bool": {
                            "should": record_ids_query
                        }
                    }
                ]
            }
        }
    });

    // delete records
    println!("Query: {}", serde_json::to_string_pretty(&query).unwrap());
    // let response = client
    //     .delete_by_query(DeleteByQueryParts::Index(&[index]))
    //     .body(query)
    //     .send()
    //     .await?;

    // println!("{:?}", response);

    //println!("Query: {}", serde_json::to_string_pretty(&query).unwrap());
    //println!("Query: {}", query);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    loop{
    let client = Elasticsearch::new(create_transport()?);

    let index = ".ds-logs-fim.event-default*";

    let (tx, mut rx) = mpsc::channel(1024);
    let client_clone = client.clone();
    let handle = tokio::spawn(async {
        get_aggs_entries_from_index(client_clone, index, 2048, tx).await.unwrap();
        
    });

    let mut handles = Vec::new();
    let returns_vec = Arc::new(Mutex::new(Vec::new()));

    let buffer_length = 1;
    while let Some(value) = rx.recv().await {
        if value.get("doc_count").unwrap().as_i64().unwrap() > 1 {
            let value_clone = value.clone();
            let returns_vec_clone = Arc::clone(&returns_vec);

            let handle = tokio::spawn(async move {
                let (file_path, record_id_and_index, event_type) = handle_record(&index, value_clone).await.unwrap();
                returns_vec_clone.lock().unwrap().push((file_path, record_id_and_index, event_type));
                //handle_record(&index, value_clone).await;
            });
            handles.push(handle);


            if let Ok(mut returns_vec_guard) = returns_vec.lock() {
                if returns_vec_guard.len() >= buffer_length {
                    let mut file_paths = HashSet::new();
                    let mut record_ids = HashSet::new();
            
                    //unwrap the values from the tuple
                    let returns_vec_temp = returns_vec_guard.clone();
            
                    for (file_path, record_id_and_index, _event_type) in returns_vec_temp {
                        file_paths.insert(file_path);
                        record_ids.insert(record_id_and_index);
                    }

                    //delete_records(index, file_paths, record_ids).await.unwrap();

                    returns_vec_guard.clear();
            
                    println!("File Paths: {:?}", &file_paths);
                    println!("Record IDs: {:?}", &record_ids);
                }
            } else {
                println!("Unable to acquire lock on returns_vec");
            }
        }
  
    }

    // Wait for all tasks to finish
    for handle in handles {
        handle.await?;
    }

    // Wait for the initial task to finish
    handle.await?;
}

    Ok(())
}
