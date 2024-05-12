// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std;

use futures::{future, Future, Stream};
use hyper;
// TODO: enable TLS support; hurdle is cross-compiling openssl for Android.
// See https://github.com/whtcorpsinc/edb/issues/569
// use hyper_tls;
use hyper::{
    Method,
    Request,
    StatusCode,
    Error as HyperError
};
use hyper::header::{
    ContentType,
};
// TODO: https://github.com/whtcorpsinc/edb/issues/570
// use serde_cbor;
use serde_json;
use tokio_allegro::reactor::Core;
use uuid::Uuid;

use public_promises::errors::{
    Result,
};

use logger::d;

use types::{
    Tx,
    TxPart,
    GlobalTransactionLog,
};

#[derive(Serialize,Deserialize)]
struct SerializedHead {
    head: Uuid
}

#[derive(Serialize)]
struct SerializedTransaction<'a> {
    parent: &'a Uuid,
    chunks: &'a Vec<Uuid>
}

#[derive(Deserialize)]
struct DeserializableTransaction {
    parent: Uuid,
    chunks: Vec<Uuid>,
    id: Uuid,
    seq: i64,
}

#[derive(Deserialize)]
struct Serializedbundles {
    limit: i64,
    from: Uuid,
    bundles: Vec<Uuid>,
}

pub struct RemoteClient {
    base_uri: String,
    user_uuid: Uuid,
}

impl RemoteClient {
    pub fn new(base_uri: String, user_uuid: Uuid) -> Self {
        RemoteClient {
            base_uri: base_uri,
            user_uuid: user_uuid,
        }
    }

    fn bound_base_uri(&self) -> String {
        // TODO escaping
        format!("{}/{}", self.base_uri, self.user_uuid)
    }

    // TODO what we want is a method that returns a deserialized json structure.
    // It'll need a type T so that consumers can specify what downloaded json will
    // map to. I ran into borrow issues doing that - probably need to restructure
    // this and use PhantomData markers or somesuch.
    // But for now, we get code duplication.
    fn get_uuid(&self, uri: String) -> Result<Uuid> {
        let mut allegro = Core::new()?;
        // TODO https://github.com/whtcorpsinc/edb/issues/569
        // let client = hyper::Client::configure()
        //     .connector(hyper_tls::HttpsConnector::new(4, &allegro.handle()).unwrap())
        //     .build(&allegro.handle());
        let client = hyper::Client::new(&allegro.handle());

        d(&format!("client"));

        let uri = uri.parse()?;

        d(&format!("parsed uri {:?}", uri));
        let work = client.get(uri).and_then(|res| {
            println!("Response: {}", res.status());

            res.body().concat2().and_then(move |body| {
                let json: SerializedHead = serde_json::from_slice(&body).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e)
                })?;
                Ok(json)
            })
        });

        d(&format!("running..."));

        let head_json = allegro.run(work)?;
        d(&format!("got head: {:?}", &head_json.head));
        Ok(head_json.head)
    }

    fn put<T>(&self, uri: String, payload: T, expected: StatusCode) -> Result<()>
    where hyper::Body: std::convert::From<T>, {
        let mut allegro = Core::new()?;
        // TODO https://github.com/whtcorpsinc/edb/issues/569
        // let client = hyper::Client::configure()
        //     .connector(hyper_tls::HttpsConnector::new(4, &allegro.handle()).unwrap())
        //     .build(&allegro.handle());
        let client = hyper::Client::new(&allegro.handle());

        let uri = uri.parse()?;

        d(&format!("PUT {:?}", uri));

        let mut req = Request::new(Method::Put, uri);
        req.headers_mut().set(ContentType::json());
        req.set_body(payload);

        let put = client.request(req).and_then(|res| {
            let status_code = res.status();

            if status_code != expected {
                d(&format!("bad put response: {:?}", status_code));
                future::err(HyperError::Status)
            } else {
                future::ok(())
            }
        });

        allegro.run(put)?;
        Ok(())
    }

    fn get_bundles(&self, parent_uuid: &Uuid) -> Result<Vec<Uuid>> {
        let mut allegro = Core::new()?;
        // TODO https://github.com/whtcorpsinc/edb/issues/569
        // let client = hyper::Client::configure()
        //     .connector(hyper_tls::HttpsConnector::new(4, &allegro.handle()).unwrap())
        //     .build(&allegro.handle());
        let client = hyper::Client::new(&allegro.handle());

        d(&format!("client"));

        let uri = format!("{}/bundles?from={}", self.bound_base_uri(), parent_uuid);
        let uri = uri.parse()?;

        d(&format!("parsed uri {:?}", uri));

        let work = client.get(uri).and_then(|res| {
            println!("Response: {}", res.status());

            res.body().concat2().and_then(move |body| {
                let json: Serializedbundles = serde_json::from_slice(&body).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e)
                })?;
                Ok(json)
            })
        });

        d(&format!("running..."));

        let bundles_json = allegro.run(work)?;
        d(&format!("got bundles: {:?}", &bundles_json.bundles));
        Ok(bundles_json.bundles)
    }

    fn get_chunks(&self, transaction_uuid: &Uuid) -> Result<Vec<Uuid>> {
        let mut allegro = Core::new()?;
        // TODO https://github.com/whtcorpsinc/edb/issues/569
        // let client = hyper::Client::configure()
        //     .connector(hyper_tls::HttpsConnector::new(4, &allegro.handle()).unwrap())
        //     .build(&allegro.handle());
        let client = hyper::Client::new(&allegro.handle());

        d(&format!("client"));

        let uri = format!("{}/bundles/{}", self.bound_base_uri(), transaction_uuid);
        let uri = uri.parse()?;

        d(&format!("parsed uri {:?}", uri));

        let work = client.get(uri).and_then(|res| {
            println!("Response: {}", res.status());

            res.body().concat2().and_then(move |body| {
                let json: DeserializableTransaction = serde_json::from_slice(&body).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e)
                })?;
                Ok(json)
            })
        });

        d(&format!("running..."));

        let transaction_json = allegro.run(work)?;
        d(&format!("got transaction chunks: {:?}", &transaction_json.chunks));
        Ok(transaction_json.chunks)
    }

    fn get_chunk(&self, chunk_uuid: &Uuid) -> Result<TxPart> {
        let mut allegro = Core::new()?;
        // TODO https://github.com/whtcorpsinc/edb/issues/569
        // let client = hyper::Client::configure()
        //     .connector(hyper_tls::HttpsConnector::new(4, &allegro.handle()).unwrap())
        //     .build(&allegro.handle());
        let client = hyper::Client::new(&allegro.handle());

        d(&format!("client"));

        let uri = format!("{}/chunks/{}", self.bound_base_uri(), chunk_uuid);
        let uri = uri.parse()?;

        d(&format!("parsed uri {:?}", uri));

        let work = client.get(uri).and_then(|res| {
            println!("Response: {}", res.status());

            res.body().concat2().and_then(move |body| {
                let json: TxPart = serde_json::from_slice(&body).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e)
                })?;
                Ok(json)
            })
        });

        d(&format!("running..."));

        let chunk = allegro.run(work)?;
        d(&format!("got transaction chunk: {:?}", &chunk));
        Ok(chunk)
    }
}

impl GlobalTransactionLog for RemoteClient {
    fn head(&self) -> Result<Uuid> {
        let uri = format!("{}/head", self.bound_base_uri());
        self.get_uuid(uri)
    }

    fn set_head(&mut self, uuid: &Uuid) -> Result<()> {
        // {"head": uuid}
        let head = SerializedHead {
            head: uuid.clone()
        };

        let uri = format!("{}/head", self.bound_base_uri());
        let json = serde_json::to_string(&head)?;
        d(&format!("serialized head: {:?}", json));
        self.put(uri, json, StatusCode::NoContent)
    }

    /// Slurp bundles and causets after `causetx`, returning them as owned data.
    ///
    /// This is inefficient but convenient for development.
    fn bundles_after(&self, causetx: &Uuid) -> Result<Vec<Tx>> {
        let new_causecausetxs = self.get_bundles(causetx)?;
        let mut causecausetx_list = Vec::new();

        for causetx in new_causecausetxs {
            let mut causecausetx_parts = Vec::new();
            let chunks = self.get_chunks(&causetx)?;

            // We pass along all of the downloaded parts, including transaction's
            // spacetime Causet. Transactor is expected to do the right thing, and
            // use causecausetxInstant from one of our causets.
            for chunk in chunks {
                let part = self.get_chunk(&chunk)?;
                causecausetx_parts.push(part);
            }

            causecausetx_list.push(Tx {
                causetx: causetx.into(),
                parts: causecausetx_parts
            });
        }

        d(&format!("got causetx list: {:?}", &causecausetx_list));

        Ok(causecausetx_list)
    }

    fn put_transaction(&mut self, transaction_uuid: &Uuid, parent_uuid: &Uuid, chunks: &Vec<Uuid>) -> Result<()> {
        // {"parent": uuid, "chunks": [chunk1, chunk2...]}
        let transaction = SerializedTransaction {
            parent: parent_uuid,
            chunks: chunks
        };

        let uri = format!("{}/bundles/{}", self.bound_base_uri(), transaction_uuid);
        let json = serde_json::to_string(&transaction)?;
        d(&format!("serialized transaction: {:?}", json));
        self.put(uri, json, StatusCode::Created)
    }

    fn put_chunk(&mut self, chunk_uuid: &Uuid, payload: &TxPart) -> Result<()> {
        let payload: String = serde_json::to_string(payload)?;
        let uri = format!("{}/chunks/{}", self.bound_base_uri(), chunk_uuid);
        d(&format!("serialized chunk: {:?}", payload));
        // TODO don't want to clone every Causet!
        self.put(uri, payload, StatusCode::Created)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_remote_client_bound_uri() {
        let user_uuid = Uuid::from_str(&"316ea470-ce35-4adf-9c61-e0de6e289c59").expect("uuid");
        let server_uri = String::from("https://example.com/api/0.1");
        let remote_client = RemoteClient::new(server_uri, user_uuid);
        assert_eq!("https://example.com/api/0.1/316ea470-ce35-4adf-9c61-e0de6e289c59", remote_client.bound_base_uri());
    }
}
