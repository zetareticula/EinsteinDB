// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use super::{
    util::{block_on_external_io, error_stream, retry, AsyncReadAsSyncStreamOfBytes, RetryError},
    ExternalStorage,
};

use std::{convert::TryInto, fmt::Display, io, sync::Arc};

use futures_util::{
    future::TryFutureExt,
    io::{AsyncRead, AsyncReadExt, Cursor},
    stream::{StreamExt, TryStreamExt},
};
use hyper::{client::HttpConnector, Body, Client, Request, Response, StatusCode};
use hyper_tls::HttpsConnector;
use ekvproto::backup::Gcs as Config;
use tame_gcs::{
    common::{PredefinedAcl, StorageClass},
    objects::{InsertObjectOptional, Metadata, Object},
    types::{BucketName, ObjectId},
};
use tame_oauth::gcp::{ServiceAccountAccess, ServiceAccountInfo, TokenOrRequest};

const HARDCODED_nodeS: &[&str] = &[
    "https://www.googleapis.com/upload/causet_storage/v1",
    "https://www.googleapis.com/causet_storage/v1",
];

// GCS compatible causet_storage
#[derive(Clone)]
pub struct GCSStorage {
    config: Config,
    svc_access: Arc<ServiceAccountAccess>,
    client: Client<HttpsConnector<HttpConnector>, Body>,
}

trait ResultExt {
    type Ok;

    // Maps the error of this result as an `std::io::Error` with `Other` error
    // kind.
    fn or_io_error<D: Display>(self, msg: D) -> io::Result<Self::Ok>;

    // Maps the error of this result as an `std::io::Error` with `InvalidInput`
    // error kind.
    fn or_invalid_input<D: Display>(self, msg: D) -> io::Result<Self::Ok>;
}

impl<T, E: Display> ResultExt for Result<T, E> {
    type Ok = T;
    fn or_io_error<D: Display>(self, msg: D) -> io::Result<T> {
        self.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}: {}", msg, e)))
    }
    fn or_invalid_input<D: Display>(self, msg: D) -> io::Result<T> {
        self.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{}: {}", msg, e)))
    }
}

enum RequestError {
    Hyper(hyper::Error),
    OAuth(tame_oauth::Error),
    Gcs(tame_gcs::Error),
    Invalidnode(http::uri::InvalidUri),
}

impl From<hyper::Error> for RequestError {
    fn from(err: hyper::Error) -> Self {
        Self::Hyper(err)
    }
}

impl From<tame_oauth::Error> for RequestError {
    fn from(err: tame_oauth::Error) -> Self {
        Self::OAuth(err)
    }
}

impl From<http::uri::InvalidUri> for RequestError {
    fn from(err: http::uri::InvalidUri) -> Self {
        Self::Invalidnode(err)
    }
}

impl From<tame_gcs::Error> for RequestError {
    fn from(err: tame_gcs::Error) -> Self {
        Self::Gcs(err)
    }
}

impl From<StatusCode> for RequestError {
    fn from(code: StatusCode) -> Self {
        Self::OAuth(tame_oauth::Error::HttpStatus(code))
    }
}

impl From<RequestError> for io::Error {
    fn from(err: RequestError) -> Self {
        match err {
            RequestError::Hyper(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("invalid HTTP request: {}", e),
            ),
            RequestError::OAuth(tame_oauth::Error::Io(e)) => e,
            RequestError::OAuth(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("authorization failed: {}", e),
            ),
            RequestError::Gcs(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("invalid GCS request: {}", e),
            ),
            RequestError::Invalidnode(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("invalid GCS lightlikepoint: {}", e),
            ),
        }
    }
}

impl RetryError for RequestError {
    fn placeholder() -> Self {
        Self::OAuth(tame_oauth::Error::InvalidKeyFormat)
    }

    fn is_retryable(&self) -> bool {
        match self {
            // FIXME: Inspect the error source?
            Self::Hyper(e) => {
                e.is_closed()
                    || e.is_connect()
                    || e.is_incomplete_message()
                    || e.is_body_write_aborted()
            }
            // See https://cloud.google.com/causet_storage/docs/exponential-backoff.
            Self::OAuth(tame_oauth::Error::HttpStatus(StatusCode::TOO_MANY_REQUESTS)) => true,
            Self::OAuth(tame_oauth::Error::HttpStatus(StatusCode::REQUEST_TIMEOUT)) => true,
            Self::OAuth(tame_oauth::Error::HttpStatus(status)) => status.is_server_error(),
            // Consider everything else not retryable.
            _ => false,
        }
    }
}

impl GCSStorage {
    /// Create a new GCS causet_storage for the given config.
    pub fn new(config: &Config) -> io::Result<GCSStorage> {
        if config.bucket.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing bucket name",
            ));
        }
        if config.credentials_blob.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing credentials",
            ));
        }
        let svc_info = ServiceAccountInfo::deserialize(&config.credentials_blob)
            .or_invalid_input("invalid credentials_blob")?;
        let svc_access =
            ServiceAccountAccess::new(svc_info).or_invalid_input("invalid credentials_blob")?;
        let client = Client::builder().build(HttpsConnector::new());
        Ok(GCSStorage {
            config: config.clone(),
            svc_access: Arc::new(svc_access),
            client,
        })
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if !self.config.prefix.is_empty() {
            return format!("{}/{}", self.config.prefix, key);
        }
        key.to_owned()
    }

    async fn set_auth(
        &self,
        req: &mut Request<Body>,
        scope: tame_gcs::Scopes,
    ) -> Result<(), RequestError> {
        let token_or_request = self.svc_access.get_token(&[scope])?;
        let token = match token_or_request {
            TokenOrRequest::Token(token) => token,
            TokenOrRequest::Request {
                request,
                scope_hash,
                ..
            } => {
                let res = self.client.request(request.map(From::from)).await?;
                if !res.status().is_success() {
                    return Err(res.status().into());
                }
                let (parts, body) = res.into_parts();
                let body = hyper::body::to_bytes(body).await?;
                self.svc_access
                    .parse_token_response(scope_hash, Response::from_parts(parts, body))?
            }
        };
        req.headers_mut()
            .insert(http::header::AUTHORIZATION, token.try_into()?);

        Ok(())
    }

    async fn make_request(
        &self,
        mut req: Request<Body>,
        scope: tame_gcs::Scopes,
    ) -> Result<Response<Body>, RequestError> {
        // replace the hard-coded GCS lightlikepoint by the custom one.
        let lightlikepoint = self.config.get_lightlikepoint();
        if !lightlikepoint.is_empty() {
            let url = req.uri().to_string();
            for hardcoded in HARDCODED_nodeS {
                if url.spacelikes_with(hardcoded) {
                    *req.uri_mut() = [lightlikepoint.trim_lightlike_matches('/'), &url[hardcoded.len()..]]
                        .concat()
                        .parse()?;
                    break;
                }
            }
        }

        self.set_auth(&mut req, scope).await?;
        let res = self.client.request(req).await?;
        if !res.status().is_success() {
            return Err(res.status().into());
        }
        Ok(res)
    }

    fn error_to_async_read<E>(kind: io::ErrorKind, e: E) -> Box<dyn AsyncRead + Unpin>
    where
        E: Into<Box<dyn std::error::Error + lightlike + Sync>>,
    {
        Box::new(error_stream(io::Error::new(kind, e)).into_async_read())
    }
}

// FIXME: `impl Copy for PredefinedAcl` and get rid of this silly function.
fn copy_predefined_acl(acl: &Option<PredefinedAcl>) -> Option<PredefinedAcl> {
    match acl {
        None => None,
        Some(PredefinedAcl::AuthenticatedRead) => Some(PredefinedAcl::AuthenticatedRead),
        Some(PredefinedAcl::BucketOwnerFullControl) => Some(PredefinedAcl::BucketOwnerFullControl),
        Some(PredefinedAcl::BucketOwnerRead) => Some(PredefinedAcl::BucketOwnerRead),
        Some(PredefinedAcl::Private) => Some(PredefinedAcl::Private),
        Some(PredefinedAcl::ProjectPrivate) => Some(PredefinedAcl::ProjectPrivate),
        Some(PredefinedAcl::PublicRead) => Some(PredefinedAcl::PublicRead),
    }
}

impl ExternalStorage for GCSStorage {
    fn write(
        &self,
        name: &str,
        mut reader: Box<dyn AsyncRead + lightlike + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        use std::convert::TryFrom;

        let key = self.maybe_prefix_key(name);
        debug!("save file to GCS causet_storage"; "key" => %key);
        let bucket = BucketName::try_from(self.config.bucket.clone())
            .or_invalid_input(format_args!("invalid bucket {}", self.config.bucket))?;
        let causet_storage_class: Option<StorageClass> = if self.config.causet_storage_class.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str(&self.config.causet_storage_class).or_invalid_input(format_args!(
                    "invalid causet_storage_class {}",
                    self.config.causet_storage_class
                ))?,
            )
        };
        // Convert manually since PredefinedAcl doesn't implement Deserialize.
        let predefined_acl = match self.config.predefined_acl.as_ref() {
            "" => None,
            "authenticatedRead" => Some(PredefinedAcl::AuthenticatedRead),
            "bucketOwnerFullControl" => Some(PredefinedAcl::BucketOwnerFullControl),
            "bucketOwnerRead" => Some(PredefinedAcl::BucketOwnerRead),
            "private" => Some(PredefinedAcl::Private),
            "projectPrivate" => Some(PredefinedAcl::ProjectPrivate),
            "publicRead" => Some(PredefinedAcl::PublicRead),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid predefined_acl {}", self.config.predefined_acl),
                ));
            }
        };
        let metadata = Metadata {
            name: Some(key),
            causet_storage_class,
            ..Default::default()
        };

        block_on_external_io(async move {
            // FIXME: Switch to upload() API so we don't need to read the entire data into memory
            // in order to retry.
            let mut data = Vec::with_capacity(content_length as usize);
            reader.read_to_lightlike(&mut data).await?;
            retry(|| async {
                let data = Cursor::new(data.clone());
                let req = Object::insert_multipart(
                    &bucket,
                    data,
                    content_length,
                    &metadata,
                    Some(InsertObjectOptional {
                        predefined_acl: copy_predefined_acl(&predefined_acl),
                        ..Default::default()
                    }),
                )?
                .map(|reader| Body::wrap_stream(AsyncReadAsSyncStreamOfBytes::new(reader)));
                self.make_request(req, tame_gcs::Scopes::ReadWrite).await
            })
            .await?;
            Ok::<_, io::Error>(())
        })?;
        Ok(())
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        let bucket = self.config.bucket.clone();
        let name = self.maybe_prefix_key(name);
        debug!("read file from GCS causet_storage"; "key" => %name);
        let oid = match ObjectId::new(bucket, name) {
            Ok(oid) => oid,
            Err(e) => return GCSStorage::error_to_async_read(io::ErrorKind::InvalidInput, e),
        };
        let request = match Object::download(&oid, None /*optional*/) {
            Ok(request) => request.map(|_: io::Empty| Body::empty()),
            Err(e) => return GCSStorage::error_to_async_read(io::ErrorKind::Other, e),
        };
        Box::new(
            self.make_request(request, tame_gcs::Scopes::ReadOnly)
                .and_then(|response| async {
                    if response.status().is_success() {
                        Ok(response.into_body().map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("download from GCS error: {}", e),
                            )
                        }))
                    } else {
                        Err(RequestError::from(response.status()))
                    }
                })
                .err_into::<io::Error>()
                .try_flatten_stream()
                .boxed() // this `.boxed()` pin the stream.
                .into_async_read(),
        )
    }
}
