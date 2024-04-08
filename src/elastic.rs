use url::Url;
use std::sync::Mutex;
use lazy_static::lazy_static;

use elasticsearch::{http::transport::Transport,Elasticsearch,http::transport::TransportBuilder};
use std::error::Error;

// lazy_static! {
//     static ref ES_CLIENT: Mutex<Result<Elasticsearch, Box<dyn Error + Send>>> = Mutex::new(create_client());
// }

lazy_static! {
    static ref ES_CLIENT: Mutex<ESClient> = Mutex::new(ESClient::new().unwrap());
}

struct Host {
    host_ip: &'static str,
    host_port: u16,
    host_scheme: &'static str,
    verify_certs: bool,
    ca_certs: Option<&'static str>,
    ssl_show_warn: bool,
}

pub struct ESClient {
    pub client: Elasticsearch,
}

impl ESClient {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        let client = ESClient::create_clients()?;
        Ok(ESClient {
            client
        })
    }


    fn create_transports() -> Result<Transport, Box<dyn Error>> {

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

    pub fn create_clients() -> Result<Elasticsearch, Box<dyn Error>> {
        let transport = ESClient::create_transports()?;
        let client = Elasticsearch::new(transport);
        Ok(client)
    }

    pub fn get_instance() -> &'static Mutex<ESClient> {
        &ES_CLIENT
    }

}



// pub fn get_client() -> Result<Elasticsearch, Box<dyn Error + Send>> {
//     let lock = ES_CLIENT.lock().unwrap();
//     let client = lock.as_ref().map(|client| client.clone())?;
//     Ok(client)
// }

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

pub fn create_client() -> Result<Elasticsearch, Box<dyn Error>> {
    let transport = create_transport()?;
    let client = Elasticsearch::new(transport);
    Ok(client)
}