use serde_json::Value;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use std::collections::HashSet;
use elasticsearch::DeleteByQueryParts;

use crate::elastic::create_client;

pub async fn delete_records_from_index(index: &str, buffer_size: usize, timeout: u64, mut delete_rx: mpsc::Receiver<Value>) -> Result<(), Box<dyn std::error::Error>> {

    let mut file_paths = HashSet::new();
    let mut records = HashSet::new();

    println!("Delete records from index: {}", index);
    loop {
        tokio::select! {
            // Wait for a new record or timeout
            record = delete_rx.recv() => {
                println!("[+] record: {:?}", record);
                if let Some(record) = record {
                    let file_path = record
                        .get("file_path")
                        .and_then(|v| v.as_str())
                        .unwrap_or("empty_file_path")
                        .to_string();
    
                    let record_id = record
                        .get("record_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("empty_record_id")
                        .to_string();
    
                    let record_index = record
                        .get("record_index")
                        .and_then(|v| v.as_str())
                        .unwrap_or("empty_record_index")
                        .to_string();
    
                    file_paths.insert(file_path);
                    records.insert((record_id, record_index));
                }
            }
            // Timeout after 5 seconds
            _ = sleep(Duration::from_secs(timeout)) => {
                println!("[+] [+] Timeout [+] [+]");
                if !file_paths.is_empty() || !records.is_empty() {

                    let query = generate_query(&file_paths, &records).unwrap();

                    let response = delete_records(index, query).await?;

                    println!("Response: {}", serde_json::to_string_pretty(&response).unwrap());

                    // delete records


                    println!("[+] [+] Timeout");
                    println!("[+] [+] Deleting records: {:?}", file_paths);
                    println!("[+] [+] Deleting records: {:?}", records);
                    // delete records
                    file_paths.clear();
                    records.clear();
                }
            }
        }
    
        if file_paths.len() > buffer_size {
            println!("[+] [+] Deleting records: {:?}", file_paths);
            println!("[+] [+] Deleting records: {:?}", records);
            let query = generate_query(&file_paths, &records).unwrap();
            let response = delete_records(index, query).await?;
            println!("Response: {}", serde_json::to_string_pretty(&response).unwrap());
            // delete records
            file_paths.clear();
            records.clear();
        }
    }

}


fn generate_query(file_paths: &HashSet<String>, records: &HashSet<(String,String)>) -> Result<Value, Box<dyn std::error::Error>> {
    let mut file_paths_query = vec![];
    let mut records_query = vec![];

    for file_path in file_paths {
        file_paths_query.push(json!({
            "term": {
                "file.path": file_path
            }
        }));

        file_paths_query.push(json!({
            "wildcard": {
                "file.path": {
                    "value": format!("{}/*", file_path)
                }
            }
        }));
    }

    for (record_id, record_index) in records {
        records_query.push(json!({
            "bool": {
                "must": [
                    {
                        "term": {
                            "_id": record_id
                        }
                    },
                    {
                        "term": {
                            "_index": record_index
                        }
                    }
                ]
            }
        }));
    }

    let query = json!({
    "query": {
        "bool": {
            "should": file_paths_query,
            "must_not": records_query
        }
    }


    });

    Ok(query)
}

async fn delete_records(index: &str, query: Value) -> Result<Value, Box<dyn std::error::Error>> {
   let client = create_client()?;

   let response = client
            .delete_by_query(DeleteByQueryParts::Index(&[index]))
            .body(query)
            .send()
            .await?;

    let json_response = response.json::<Value>().await?;

    Ok(json_response)

}