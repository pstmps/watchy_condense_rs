use std::io::Read;
use url::Url;

use elasticsearch::{http::transport::Transport, http::transport::TransportBuilder, Elasticsearch};
use std::error::Error;

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
        ssl_show_warn: Option<bool>,
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

fn create_transport(es_host: Host) -> Result<Transport, Box<dyn Error>> {
    let connection_pool =
        elasticsearch::http::transport::SingleNodeConnectionPool::new(es_host.url()?);
    let credentials = elasticsearch::auth::Credentials::Basic(
        es_host.user.to_string(),
        es_host.password.to_string(),
    );
    let cert = get_certificate_validation(&es_host.cert_path)?;

    let transport = TransportBuilder::new(connection_pool)
        .auth(credentials)
        .cert_validation(cert)
        .build()?;
    Ok(transport)
}

fn get_certificate_validation(
    cert_path: &str,
) -> Result<elasticsearch::cert::CertificateValidation, Box<dyn Error>> {
    // check if the cert_path is empty, if it is, return None, otherwise read the cert file and return the Certificate
    match cert_path.is_empty() {
        true => Ok(elasticsearch::cert::CertificateValidation::None),
        false => {
            let mut buf = Vec::new();
            std::fs::File::open(cert_path)?.read_to_end(&mut buf)?;
            let raw_cert = elasticsearch::cert::Certificate::from_pem(&buf)?;
            Ok(elasticsearch::cert::CertificateValidation::Certificate(
                raw_cert,
            ))
        }
    }
}

pub fn create_client(es_host: Host) -> Result<Elasticsearch, Box<dyn Error>> {
    let transport = create_transport(es_host)?;
    let client = Elasticsearch::new(transport);
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_client_with_cert() {
        use dotenv::dotenv;
        use std::env;

        dotenv().ok();

        let es_ip = env::var("ES_IP").ok();
        let es_port = env::var("ES_PORT").ok();

        let cert_path = env::var("CERT_PATH").ok();

        let es_user = env::var("ES_USER").ok();
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
            Some(true),
        );

        let client = create_client(es_host).expect("Failed to create Elasticsearch client");

        let response = client
            .cat()
            .health()
            .format("json")
            .send()
            .await
            .expect("Failed to send health check request");

        assert_eq!(response.status_code(), 200);
    }

    #[tokio::test]
    async fn test_create_client_without_cert() {
        use dotenv::dotenv;
        use std::env;

        dotenv().ok();

        let es_ip = env::var("ES_IP").ok();
        let es_port = env::var("ES_PORT").ok();

        let es_user = env::var("ES_USER").ok();
        let es_password = env::var("ES_PASSWORD").ok();

        let es_host = Host::new(
            es_user,
            es_password,
            es_ip,
            es_port.map(|p| p.parse::<u16>().unwrap()),
            Some("https".to_string()),
            Some("".to_string()),
            Some(false),
            None,
            Some(false),
        );

        let client = create_client(es_host).expect("Failed to create Elasticsearch client");

        let response = client
            .cat()
            .health()
            .format("json")
            .send()
            .await
            .expect("Failed to send health check request");

        assert_eq!(response.status_code(), 200);
    }
}
