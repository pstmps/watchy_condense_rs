use tokio::sync::mpsc;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

use crate::message::Message;
use crate::aggs::get_aggs_entries_from_index;
use crate::latest::get_last_event_for_record;
use crate::parse_record::parse_record;
use crate::delete_records::delete_records_from_index;

pub struct App {
    pub should_quit: bool,
    pub should_suspend: bool,
    pub action_buffer_size: usize,
    pub file_paths: HashSet<String>,
    pub records: HashMap<String, String>,
  }

impl App {
    pub fn new(action_buffer_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
    
      Ok(Self {
        should_quit: false,
        should_suspend: false,
        action_buffer_size: action_buffer_size,
        file_paths: HashSet::new(),
        records: HashMap::new(),
      })
    }
  
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (event_tx, mut event_rx) = mpsc::channel(self.action_buffer_size);
        let (delete_tx, mut delete_rx) = mpsc::channel(self.action_buffer_size);

        let index = ".ds-logs-fim.event-default*";
        let page_size = 10;
        let buffer_size = 100;

        let del_timeout = 5;
        let agg_timeout = 20;

        // let _ = delete_records_from_index(index, buffer_size, delete_rx);
        let _del_handle = tokio::spawn(async move {
            delete_records_from_index(index, buffer_size, del_timeout, delete_rx).await;
        });

        let _event_tx = event_tx.clone();
        let _agg_handle = tokio::spawn(async move {
            let _ = get_aggs_entries_from_index(index, page_size, agg_timeout,_event_tx).await;
        });

        loop {

            if let Some(event) = event_rx.recv().await {
                let _event_tx = event_tx.clone();
 
                match event {
                    Message::Aggregate { event_type, payload } => {
                        //println!("Aggregate event: {} with payload: {}", &event_type, &payload);
                        let _payload = payload.clone();
                        let record = _payload["key"]["file"].to_owned();
                        let _ = tokio::spawn(async move {
                            let _ = get_last_event_for_record(index, record.as_str().unwrap(), _event_tx).await;
                        });
                    },
                    Message::LastRecord { event_type, payload } => {
                        //println!("LastRecord event: {} with payload: {}", event_type, payload);
                        let _payload = payload.clone();
                        let _ = tokio::spawn(async move {
                            let _ = parse_record( _payload, _event_tx).await;
                        });

                    },
                    Message::Delete { event_type, payload } => {

                        let _delete_tx = delete_tx.clone();
                        //println!("Delete event: {} with payload: {}", event_type, payload);
                        let _ = tokio::spawn(async move {
                            let _ = _delete_tx.send(payload).await;
                        });

                    },
                }

            }


        if self.should_quit {
          return Ok(())
        }
  
        if self.should_suspend {
            return Ok(())
        }
    }
    }

   

}