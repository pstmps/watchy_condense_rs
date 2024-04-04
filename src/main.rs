pub mod app;
pub mod message;
pub mod elastic;
pub mod aggs;
pub mod latest;
pub mod parse_record;
pub mod delete_records;

use crate::app::App;


async fn tokio_main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: initialize_logging()?;
  
    // TODO initialize_panic_handler()?;
  
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