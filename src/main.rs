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
// use crate::elastic::ESClient;
use elasticsearch::http::response::Response;


async fn tokio_main() -> Result<(), Box<dyn std::error::Error>> {
    initialize_logging("/Users/stiebing/Documents/scripting_base/watchy_condense_rs/log")?;

    use dotenv::dotenv;
    use std::env;

    dotenv().ok();

    let es_ip = env::var("ES_IP").ok();
    let es_port = env::var("ES_PORT").ok();

    let cert_path = env::var("CERT_PATH").ok();

    let es_user = env::var("ES_USERD").ok();
    let es_password = env::var("ES_PASSWORD").ok();

    let es_host = elastic::Host::new(
        es_user,
        es_password,
        es_ip,
        es_port.map(|p| p.parse::<u16>().unwrap()),
        Some("https".to_string()),
        cert_path,
        Some(false),
        None,
        Some(false)
    );
  
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
    let mut app = App::new(es_host, 1024,".ds-logs-fim.event-default*",10,100,5,20)?;
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