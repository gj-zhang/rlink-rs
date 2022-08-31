use std::borrow::Borrow;
use std::collections::HashMap;
use std::str::FromStr;

use reqwest::{Body, Client};
use reqwest::header;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::de::DeserializeOwned;

use rlink::core::properties::Properties;

use crate::{DORIS_CONNECT_TIMEOUT_MS, DORIS_HEADER_PASSWORD, DORIS_HEADER_USERNAME};

pub async fn get<R>(url: String, prop: Properties, client: &Client) -> anyhow::Result<R>
    where R: DeserializeOwned {
    let user = prop.get_string(DORIS_HEADER_USERNAME)?;
    let password = prop.get_string(DORIS_HEADER_PASSWORD)?;
    let res = client.get(url)
        .basic_auth(user, Some(password))
        .send().await?;
    let json = res.json::<R>().await?;
    Ok(json)
}

pub async fn put<T, U>(prop: &Properties, url: &String, body: T, client: &Client) -> anyhow::Result<U>
    where U: DeserializeOwned, Body: From<T> {
    let user = prop.get_string(DORIS_HEADER_USERNAME).unwrap();
    let password = prop.get_string(DORIS_HEADER_PASSWORD).unwrap();
    let map = prop.as_map().borrow();
    let headers = header(map);
    let response = client
        .put(url)
        .basic_auth(user, Some(password))
        .headers(headers)
        .body(body)
        .send().await?
        .json::<U>().await?;

    Ok(response)
}

fn header(prop: &HashMap<String, String>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::EXPECT, "100-continue".parse().unwrap());
    headers.remove(header::CONTENT_LENGTH);
    headers.remove(header::TRANSFER_ENCODING);
    for entry in prop.into_iter() {
        headers.insert(HeaderName::from_str(entry.0.as_str()).unwrap(), HeaderValue::from_str(entry.1.as_str()).unwrap());
    }
    headers
}