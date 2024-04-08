use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use serde_json::Value;

use crate::message::Message;
use crate::aggs::get_aggs_entries_from_index;
use crate::latest::get_last_event_for_record;
use crate::parse_record::parse_record;
use crate::delete_records::delete_records_from_index;

pub struct App {
    pub should_quit: bool,
    pub should_suspend: bool,
    pub action_buffer_size: usize,
    pub index: String,
    pub page_size: usize,
    pub buffer_size: usize,
    pub del_timeout: u64,
    pub agg_timeout: u64,
  }

impl App {
    pub fn new(action_buffer_size: usize, index: &str, page_size: usize, buffer_size: usize, del_timeout: u64, agg_timeout: u64) -> Result<Self, Box<dyn std::error::Error>> {
    
      Ok(Self {
        should_quit: false,
        should_suspend: false,
        action_buffer_size: action_buffer_size,
        index: index.to_string(),
        page_size: page_size,
        buffer_size: buffer_size,
        del_timeout: del_timeout,
        agg_timeout: agg_timeout,
      })
    }
  
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (event_tx, mut event_rx) = mpsc::channel(self.action_buffer_size);
        let (delete_tx, delete_rx) = mpsc::channel(self.action_buffer_size);

        log::info!("Starting condensing app on index: {} with buffer size: {}", self.index, self.action_buffer_size);

        // let index = ".ds-logs-fim.event-default*";
        // let page_size = 10;
        // let buffer_size = 100;

        // let del_timeout = 5;
        // let agg_timeout = 20;
        let index = self.index.clone();
        let page_size = self.page_size;
        let buffer_size = self.buffer_size;
        let del_timeout = self.del_timeout;
        let agg_timeout = self.agg_timeout;

        let mut handles = Vec::new();
        let _index = index.to_string();
        let _del_handle = tokio::spawn(async move {
            if let Err(e) = delete_records_from_index(_index.as_str(), buffer_size, del_timeout, delete_rx).await {
                log::error!("Failed to start delete records from index task: {}", e)
            }
        });
        handles.push(_del_handle);

        let _event_tx = event_tx.clone();
        let _index = index.to_string();
        let _agg_handle = tokio::spawn(async move {

            if let Err(e) = get_aggs_entries_from_index(_index.as_str(), page_size, agg_timeout,_event_tx).await {
                log::error!("Failed to start get aggs entries from index task: {}", e);
            };
        });
        handles.push(_agg_handle);

        let _index = index.to_string();

        loop {
            if let Some(event) = event_rx.recv().await {
                if let Err(e) = self.process_events(event, &event_tx, &delete_tx, &mut handles, _index.as_str()).await {
                    log::error!("Failed to process events: {}", e);
                };
            }
            if self.should_quit {
                return Ok(())
            }
            if self.should_suspend {
                return Ok(())
            }
        }

    }

    async fn process_events(&mut self, event: Message, event_tx: &mpsc::Sender<Message>, delete_tx: &mpsc::Sender<Value>, handles: &mut Vec<JoinHandle<()>>, index: &str) -> Result<(), Box<dyn std::error::Error>> {
        let _event_tx = event_tx.clone();
        match event {
            Message::Aggregate { event_type: _event_type, payload } => {
                log::debug!("Aggregate event received: {} with payload: {}", _event_type, payload);
                let _payload = payload.clone();
                let _index = index.to_string();
                let record = _payload["key"]["file"].to_owned();
                let lastevent_handle = tokio::spawn(async move {
                    let _ = get_last_event_for_record(&_index, record.as_str().unwrap(), _event_tx).await;
                });
                handles.push(lastevent_handle);
            },
            Message::LastRecord { event_type: _event_type, payload } => {
                log::debug!("LastRecord event received: {} with payload: {}", _event_type, payload);
                let _payload = payload.clone();
                let parserecord_handle = tokio::spawn(async move {
                    let _ = parse_record( _payload, _event_tx).await;
                });
                handles.push(parserecord_handle);
            },
            Message::Delete { event_type: _event_type, payload } => {
                log::debug!("Delete event received: {} with payload: {}", _event_type, payload);
                let _delete_tx = delete_tx.clone();
                let deltx_handle = tokio::spawn(async move {
                    let _ = _delete_tx.send(payload).await;
                });
                handles.push(deltx_handle);
            },
        }
        Ok(())
    }

}