use url::Url;
use std::sync::Mutex;
use lazy_static::lazy_static;

use std::io::Read;

use elasticsearch::{http::transport::Transport,Elasticsearch,http::transport::TransportBuilder};
use std::error::Error;

// lazy_static! {
//     static ref ES_CLIENT: Mutex<Result<Elasticsearch, Box<dyn Error + Send>>> = Mutex::new(create_client());
// }

// lazy_static! {
//     static ref ES_CLIENT: Mutex<ESClient> = Mutex::new(ESClient::new().unwrap());
// }
#[derive(Clone)]
pub struct Host {
    user: String,
    password: String,
    host_ip: String,
    host_port: u16,
    host_scheme: String,
    cert_path: String,
    verify_certs: bool,
    ca_certs: Option<String>,
    ssl_show_warn: bool,
}

impl Host {
    pub fn new(
        user: Option<String>, 
        password: Option<String>, 
        host_ip: Option<String>, 
        host_port: Option<u16>, 
        host_scheme: Option<String>,
        cert_path: Option<String>,
        verify_certs: Option<bool>,
        ca_certs: Option<String>,
        ssl_show_warn: Option<bool>
    ) -> Self {
        Self {
            user: user.unwrap_or_else(|| "default_user".to_string()),
            password: password.unwrap_or_else(|| "default_password".to_string()),
            host_ip: host_ip.unwrap_or_else(|| "localhost".to_string()),
            host_port: host_port.unwrap_or(9200),
            host_scheme: host_scheme.unwrap_or_else(|| "http".to_string()),
            cert_path: cert_path.unwrap_or_else(|| "".to_string()),
            verify_certs: verify_certs.unwrap_or(false),
            ca_certs: ca_certs,
            ssl_show_warn: ssl_show_warn.unwrap_or(false),
        }
    }


    pub fn url(&self) -> Result<Url, Box<dyn Error>> {
        let url_str = format!("{}://{}:{}", self.host_scheme, self.host_ip, self.host_port);
        let url = Url::parse(&url_str)?;
        Ok(url)
    }
}



// pub struct ESClient {
//     pub client: Elasticsearch,
// }

// impl ESClient {
//     pub fn new() -> Result<Self, Box<dyn Error>> {
//         let client = ESClient::create_clients()?;
//         Ok(ESClient {
//             client
//         })
//     }


//     fn create_transports() -> Result<Transport, Box<dyn Error>> {

//         let es_user = "elastic";
//         let es_password = "f9qXA9bfmLCqxopJDquh";

//         let es_hosts = vec![
//             Host {
//                 host_ip: "192.168.2.193",
//                 host_port: 9200,
//                 host_scheme: "https",
//                 verify_certs: false,
//                 ca_certs: None,
//                 ssl_show_warn: false,
//             },
//         ];

//         let url = Url::parse(&format!(
//             "{}://{}:{}/",
//             es_hosts[0].host_scheme, es_hosts[0].host_ip, es_hosts[0].host_port
//         ))?;

//         let connection_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);

//         let credentials = elasticsearch::auth::Credentials::Basic(es_user.to_string(), es_password.to_string());

//         let transport = TransportBuilder::new(connection_pool)
//             .auth(credentials)
//             .cert_validation(elasticsearch::cert::CertificateValidation::None)
//             .build()?;

//         Ok(transport)
        
//     }

//     pub fn create_clients() -> Result<Elasticsearch, Box<dyn Error>> {
//         let transport = ESClient::create_transports()?;
//         let client = Elasticsearch::new(transport);
//         Ok(client)
//     }

//     pub fn get_instance() -> &'static Mutex<ESClient> {
//         &ES_CLIENT
//     }

// }



// pub fn get_client() -> Result<Elasticsearch, Box<dyn Error + Send>> {
//     let lock = ES_CLIENT.lock().unwrap();
//     let client = lock.as_ref().map(|client| client.clone())?;
//     Ok(client)
// }

fn create_transport(es_host: Host) -> Result<Transport, Box<dyn Error>> {

    // let es_user = "elastic";
    // let es_password = "f9qXA9bfmLCqxopJDquh";

    // let es_hosts = vec![
    //     Host {
    //         user: es_user,
    //         password: es_password,
    //         host_ip: "192.168.2.193",
    //         host_port: 9200,
    //         host_scheme: "https",
    //         cert_path: "/Users/stiebing/Documents/scripting_base/watchy_condense_rs/http_ca.crt",
    //         verify_certs: false,
    //         ca_certs: None,
    //         ssl_show_warn: false,
    //     },
    //     Host {
    //         user: es_user,
    //         password: es_password,
    //         host_ip: "192.168.2.194",
    //         host_port: 9200,
    //         host_scheme: "https",
    //         cert_path: "/Users/stiebing/Documents/scripting_base/watchy_condense_rs/http_ca.crt",
    //         verify_certs: false,
    //         ca_certs: None,
    //         ssl_show_warn: false,
    //     },
    // ];

    // let es_host = Host {
    //     user: es_user,
    //     password: es_password,
    //     host_ip: "

    // let mut es_urls = vec![];

    // for host in &es_hosts {
    //     es_urls.push(Url::parse(&format!(
    //         "{}://{}:{}/",
    //         host.host_scheme, host.host_ip, host.host_port
    //     ))?);
        
    // }

    // println!("{:?}", es_urls);

    // let url = Url::parse(&format!(
    //     "{}://{}:{}/",
    //     es_hosts[0].host_scheme, es_hosts[0].host_ip, es_hosts[0].host_port
    // ))?;

    let connection_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(es_host.url()?);


    let credentials = elasticsearch::auth::Credentials::Basic(es_host.user.to_string(), es_host.password.to_string());

    let cert = get_certificate_validation(&es_host.cert_path)?;

    let transport = TransportBuilder::new(connection_pool)
        .auth(credentials)
        .cert_validation(cert)
        .build()?;

    Ok(transport)
    
}

fn get_certificate_validation(cert_path: &str) -> Result<elasticsearch::cert::CertificateValidation, Box<dyn Error>> {

    match cert_path.is_empty() {
        true => Ok(elasticsearch::cert::CertificateValidation::None),
        false => {
            let mut buf = Vec::new();
            std::fs::File::open(cert_path)?
                .read_to_end(&mut buf)?;
            let raw_cert = elasticsearch::cert::Certificate::from_pem(&buf)?;
            Ok(elasticsearch::cert::CertificateValidation::Certificate(raw_cert))
        }
    }

}

pub fn create_client(es_host: Host) -> Result<Elasticsearch, Box<dyn Error>> {

    // let es_host = Host::new(
    //     Some("elastic".to_string()), 
    //     Some("f9qXA9bfmLCqxopJDquh".to_string()), 
    //     Some("192.168.2.193".to_string()),
    //     None,
    //     Some("https".to_string()),
    //     Some("/Users/stiebing/Documents/scripting_base/watchy_condense_rs/http_ca.crt".to_string()),
    //     Some(false),
    //     None,
    //     None
    // );


    let transport = create_transport(es_host)?;
    let client = Elasticsearch::new(transport);
    Ok(client)
}

#[cfg(test)]
mod tests {
    use std::f32::consts::E;

    use elasticsearch::{Elasticsearch, Error};
    use serde_json::Value;
    use super::*;

    #[tokio::test]
    async fn test_create_client() {

        use dotenv::dotenv;
        use std::env;

        dotenv().ok();

        let es_ip = env::var("ES_IP").ok();
        let es_port = env::var("ES_PORT").ok();

        let cert_path = env::var("CERT_PATH").ok();

        let es_user = env::var("ES_USERD").ok();
        let es_password = env::var("ES_PASSWORD").ok();

        let es_host = Host::new(
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


        let client = create_client(es_host).expect("Failed to create Elasticsearch client");

        let response = client
            .cat()
            .health()
            .format("json")
            .send()
            .await
            .expect("Failed to send health check request");

        println!("{:?}", &response);

        let response_body = response.json::<Value>().await.expect("Failed to parse response body");

        println!("{:?}", response_body);

        
    }
}