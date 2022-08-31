use std::time::Duration;

use rand::Rng;
use reqwest::Client;

use rlink::core::properties::Properties;

use crate::{DORIS_CONNECT_TIMEOUT_MS, http};
use crate::{DORIS_HEADER_PASSWORD, DORIS_HEADER_USERNAME};
use crate::stream_load::DorisConfigOption;

const REST_RESPONSE_STATUS_OK: i32 = 200;
const API_PREFIX: &'static str = "/api";
const SCHEMA: &'static str = "_schema";
const BACKEND_V2: &'static str = "/api/backends?is_alive=true";

#[derive(Deserialize, Serialize)]
pub struct BackendResp {
    msg: String,
    code: i32,
    data: BackendV2,
}

#[derive(Deserialize, Serialize)]
pub struct BackendV2 {
    backends: Vec<BackendRowV2>,
}

#[derive(Deserialize, Serialize)]
pub struct BackendRowV2 {
    ip: String,
    http_port: i32,
    is_alive: bool,
}

pub fn random_endpoint(fe_nodes: &String) -> anyhow::Result<String> {
    let nodes: Vec<&str> = fe_nodes.split(",").collect();
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(0, nodes.len());
    let n = nodes.get(i).unwrap();
    Ok(n.to_string())
}

pub async fn random_backend(options: &DorisConfigOption, client: &Client) -> String {
    let backends = get_backends_v2(options, client).await.unwrap();
    let be = backends.get(0).ok_or("the backends choose error").unwrap();
    format!("{}:{}", be.ip, be.http_port)
}

pub async fn get_backends_v2(options: &DorisConfigOption, client: &Client) -> anyhow::Result<Vec<BackendRowV2>> {
    let fe = &options.fe_nodes;
    let fe = random_endpoint(fe)?;
    let be_url = format!("http://{}{}", fe, BACKEND_V2);
    let mut prop = Properties::new();

    prop.set_str(DORIS_HEADER_USERNAME, options.username.as_str());
    prop.set_str(DORIS_HEADER_PASSWORD, options.password.as_str());
    prop.set_duration(DORIS_CONNECT_TIMEOUT_MS, Duration::from_millis(options.connect_timeout_ms as u64));
    let res = http::get::<BackendResp>(be_url, prop, client).await?;
    Ok(res.data.backends)
}
