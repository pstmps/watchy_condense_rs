use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::aggs::get_aggs_entries_from_index;
use crate::delete_records::delete_records_from_index;
use crate::elastic::Host;
use crate::latest::get_last_event_for_record;
use crate::message::Message;
use crate::parse_record::parse_record;

pub struct App {
    pub es_host: Host,
    pub should_quit: bool,
    pub should_suspend: bool,
    pub action_buffer_size: usize,
    pub index: String,
    pub page_size: usize,
    pub buffer_size: usize,
    pub del_timeout: u64,
    pub agg_sleep: u64,
}

impl App {
    pub fn new(
        es_host: Host,
        action_buffer_size: usize,
        index: &str,
        page_size: usize,
        buffer_size: usize,
        del_timeout: u64,
        agg_sleep: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            es_host,
            should_quit: false,
            should_suspend: false,
            action_buffer_size,
            index: index.to_string(),
            page_size,
            buffer_size,
            del_timeout,
            agg_sleep,
        })
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (event_tx, mut event_rx) = mpsc::channel(self.action_buffer_size);
        let (delete_tx, delete_rx) = mpsc::channel(self.action_buffer_size);

        log::info!(
            "Starting condensing app on index: {} with buffer size: {}",
            self.index,
            self.action_buffer_size
        );

        let index = self.index.clone();
        let page_size = self.page_size;
        let buffer_size = self.buffer_size;
        let del_timeout = self.del_timeout;
        let agg_sleep = self.agg_sleep;

        let mut handles = Vec::new();
        let _index = index.to_string();
        let _es_host = self.es_host.clone();
        let _del_handle = tokio::spawn(async move {
            if let Err(e) = delete_records_from_index(
                _es_host,
                _index.as_str(),
                buffer_size,
                del_timeout,
                delete_rx,
            )
            .await
            {
                log::error!("Failed to start delete records from index task: {}", e)
            }
        });
        handles.push(_del_handle);

        let _event_tx = event_tx.clone();
        let _index = index.to_string();
        let _es_host = self.es_host.clone();
        let _agg_handle = tokio::spawn(async move {
            if let Err(e) = get_aggs_entries_from_index(
                _es_host,
                _index.as_str(),
                page_size,
                agg_sleep,
                _event_tx,
            )
            .await
            {
                log::error!("Failed to start get aggs entries from index task: {}", e);
            };
        });
        handles.push(_agg_handle);

        let _index = index.to_string();

        loop {
            if let Some(event) = event_rx.recv().await {
                let _es_host = self.es_host.clone();
                if let Err(e) = self
                    .process_events(
                        _es_host,
                        event,
                        &event_tx,
                        &delete_tx,
                        &mut handles,
                        _index.as_str(),
                    )
                    .await
                {
                    log::error!("Failed to process events: {}", e);
                };
            }
            // if self.should_quit {
            //     return Ok(());
            // }
            // if self.should_suspend {
            //     return Ok(());
            // }
        }
    }

    async fn process_events(
        &mut self,
        es_host: Host,
        event: Message,
        event_tx: &mpsc::Sender<Message>,
        delete_tx: &mpsc::Sender<Value>,
        handles: &mut Vec<JoinHandle<()>>,
        index: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _event_tx = event_tx.clone();
        match event {
            Message::Aggregate {
                event_type: _event_type,
                payload,
            } => {
                log::debug!(
                    "Aggregate event received: {} with payload: {}",
                    _event_type,
                    payload
                );
                let _payload = payload.clone();
                let _index = index.to_string();
                let record = _payload["key"]["file"].to_owned();
                let lastevent_handle = tokio::spawn(async move {
                    // let _ = get_last_event_for_record(es_host, &_index, record.as_str().unwrap(), _event_tx).await;
                    if let Some(record_str) = record.as_str() {
                        let _ = get_last_event_for_record(es_host, &_index, record_str, _event_tx)
                            .await;
                    } else {
                        log::error!("Failed to convert record to str");
                    }
                });
                handles.push(lastevent_handle);
            }
            Message::LastRecord {
                event_type: _event_type,
                payload,
            } => {
                log::debug!(
                    "LastRecord event received: {} with payload: {}",
                    _event_type,
                    payload
                );
                let _payload = payload.clone();
                let parserecord_handle = tokio::spawn(async move {
                    let _ = parse_record(_payload, _event_tx).await;
                });
                handles.push(parserecord_handle);
            }
            Message::Delete {
                event_type: _event_type,
                payload,
            } => {
                log::debug!(
                    "Delete event received: {} with payload: {}",
                    _event_type,
                    payload
                );
                let _delete_tx = delete_tx.clone();
                let deltx_handle = tokio::spawn(async move {
                    let _ = _delete_tx.send(payload).await;
                });
                handles.push(deltx_handle);
            }
        }
        Ok(())
    }
}
