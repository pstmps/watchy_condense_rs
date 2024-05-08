use serde_json::Value;
// use std::sync::{Arc, Mutex};
// use std::sync::Arc;
// use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
// use std::time::Duration;
// use std::cell::RefCell;

// // use futures_util::future::future::FutureExt;
// use futures_util::FutureExt;
// use futures_util::task::noop_waker;

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
        // let (delete_tx, delete_rx) = mpsc::channel(self.action_buffer_size);
        let (delete_tx, _delete_rx) = broadcast::channel(self.action_buffer_size);
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

        // -ARC bool is running-
        // let is_running = Arc::new(AtomicBool::new(false));

        let mut agg_handle: Option<tokio::task::JoinHandle<()>> = None;

        let mut del_handle: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            // check if aggregation task is runnning, if not, restart it
            if let Some(handle) = &agg_handle {
                if handle.is_finished() {
                    // The task has completed, you can start a new one
                    agg_handle = None;
                }
            }

            if agg_handle.is_none() {
                let _event_tx = event_tx.clone();
                let _index_clone = index.to_string();
                let _es_host = self.es_host.clone();

                agg_handle = Some(tokio::spawn(async move {
                    loop {
                        match get_aggs_entries_from_index(
                            _es_host.clone(),
                            _index_clone.as_str(),
                            page_size,
                            agg_sleep,
                            _event_tx.clone(),
                        )
                        .await
                        {
                            Ok(_) => break, // If the function succeeds, break the loop
                            Err(e) => {
                                log::error!(
                                    "Failed to start get aggs entries from index task: {}",
                                    e
                                );
                                // Optionally, you can add a delay before retrying
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            }
                        };
                    }
                }));
            }

            // check if delete task is runnning, if not, restart it
            if let Some(handle) = &del_handle {
                if handle.is_finished() {
                    // The task has completed, you can start a new one
                    del_handle = None;
                }
            }

            if del_handle.is_none() {
                let mut _delete_rx = delete_tx.subscribe();
                let _index_clone = index.to_string();
                let _es_host = self.es_host.clone();

                del_handle = Some(tokio::spawn(async move {
                    if let Err(e) = delete_records_from_index(
                        _es_host.clone(),
                        _index_clone.as_str(),
                        buffer_size,
                        del_timeout,
                        _delete_rx,
                    )
                    .await
                    {
                        log::error!("Failed to start delete records from index task: {}", e)
                    }
                }));
            }

            // -ARC bool is running-
            // this was the first variant to check if the task is running, but while it was working, it needed a lot of extra imports
            // using the onboard tokio handle tools (.is_finished()) felt more natural

            // let is_running_clone = Arc::clone(&is_running);

            // log::debug!("Starting get aggs entries from index task: {}", _index.as_str());

            // let _event_tx = event_tx.clone();

            // let _index_clone = index.to_string();
            // let _es_host = self.es_host.clone();

            // if !is_running_clone.load(Ordering::SeqCst) {
            //     let _is_running_clone = Arc::clone(&is_running_clone);
            //     let _agg_handle = tokio::spawn(async move {
            //         _is_running_clone.store(true, Ordering::SeqCst);
            //         loop {
            //             // is_running_clone.store(true, Ordering::SeqCst);
            //             match get_aggs_entries_from_index(
            //                 _es_host.clone(),
            //                 _index_clone.as_str(),
            //                 page_size,
            //                 agg_sleep,
            //                 _event_tx.clone(),
            //             )
            //             .await
            //             {
            //                 Ok(_) => break, // If the function succeeds, break the loop
            //                 Err(e) => {
            //                     log::error!("Failed to start get aggs entries from index task: {}", e);
            //                     // Optionally, you can add a delay before retrying
            //                     tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            //                 }
            //             };
            //         }
            //         _is_running_clone.store(false, Ordering::SeqCst);
            //     });
            //     // handles.push(_agg_handle);
            // }

            // // let _ = _agg_handle.await;
            // -ARC bool is running-

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
        delete_tx: &broadcast::Sender<Value>,
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
                    log::debug!("Sending delete payload: {:?}", payload);
                    let _ = _delete_tx.send(payload);
                });
                handles.push(deltx_handle);
            }
        }
        Ok(())
    }
}
