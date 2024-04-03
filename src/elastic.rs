use url::Url;

use elasticsearch::{http::transport::Transport,Elasticsearch,http::transport::TransportBuilder};
use std::error::Error;

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

pub fn create_client() -> Result<Elasticsearch, Box<dyn Error>> {
    let transport = create_transport()?;
    let client = Elasticsearch::new(transport);
    Ok(client)
}