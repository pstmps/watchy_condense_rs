pub mod app;
pub mod message;
pub mod elastic;
pub mod aggs;
pub mod latest;
pub mod parse_record;
pub mod delete_records;
pub mod init_logging;

use crate::app::App;
use crate::init_logging::initialize_logging;
use crate::elastic::ESClient;
use elasticsearch::http::response::Response;


async fn tokio_main() -> Result<(), Box<dyn std::error::Error>> {
    initialize_logging("/Users/stiebing/Documents/scripting_base/watchy_condense_rs/log")?;
  
    // TODO initialize_panic_handler()?;

    // Create an instance of ESClient with a placeholder Elasticsearch client
    // let mut es_client = ESClient::new()?;

    // // Create an Elasticsearch client
    // // let client = es_client.create_clients()?;

    // let es_client_guard = ESClient::get_instance().lock().unwrap();
    // let client = &es_client_guard.client;

    // let response: Response = client
    // .cat()
    // .health()
    // .send()
    // .await?;

    // println!("{:?}", response);
  
    //let args = Cli::parse();
    let mut app = App::new(1024,".ds-logs-fim.event-default*",10,100,5,20)?;
    app.run().await?;
  
    Ok(())
  }
  
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
if let Err(e) = tokio_main().await {
    eprintln!("{} error: Something went wrong", env!("CARGO_PKG_NAME"));
    Err(e)
} else {
    Ok(())
}
}