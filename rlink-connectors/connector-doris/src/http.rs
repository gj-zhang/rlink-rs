use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use futures::executor::block_on;
use reqwest::header;
use reqwest::blocking::{Client, Body};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::de::DeserializeOwned;

use rlink::core::properties::Properties;

use crate::stream_load::{DORIS_HEADER_PASSWORD, DORIS_HEADER_USERNAME};
use crate::rest::BackendV2;

pub fn get<R>(url: String, prop: Properties) -> anyhow::Result<R>
    where R: DeserializeOwned {
    let connect_time_out = prop.get_duration("connect_timeout_ms")?;
    let user = prop.get_string("username")?;
    let password = prop.get_string("password")?;

    let client = Client::builder()
        .connect_timeout(connect_time_out)
        .build().expect("http client build error");

    let res = client.get(url)
        .basic_auth(user, Some(password))
        .send()?;
    let json = res.json::<R>()?;
    Ok(json)
}

// pub fn post<T, U>(prop: Properties, url: String, body: &T) -> anyhow::Result<U>
//     where U: DeserializeOwned, T: Serialize + ?Sized {
//     let client = Client::builder()
//         .connect_timeout(prop.get_duration("connect_time_out")?)
//         .build()?;
//     let user = prop.get_string(DORIS_HEADER_PASSWORD).unwrap();
//     let password = prop.get_string(DORIS_HEADER_USERNAME).unwrap();
//     let headers = header(prop);
//
//     let res_future = client.post(url)
//         .basic_auth(user, Some(password))
//         .headers(headers)
//         .body(body)
//         .send();
//     let response = block_on(res_future)?;
//
//     let res_json_future = response.json::<U>();
//     let res = block_on(res_json_future)?;
//     Ok(res)
// }

pub fn put<T, U>(prop: &Properties, url: &String, body: T) -> anyhow::Result<U>
    where U: DeserializeOwned, Body: From<T> {
    let client = Client::builder()
        .connect_timeout(prop.get_duration("connect_timeout_ms")?)
        .build()?;
    let user = prop.get_string(DORIS_HEADER_USERNAME).unwrap();
    let password = prop.get_string(DORIS_HEADER_PASSWORD).unwrap();
    let map = prop.as_map().borrow();
    let headers = header(map);
    let response = client
        .put(url)
        .basic_auth(user, Some(password))
        .headers(headers)
        .body(body)
        .send()?
        .json::<U>()?;

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