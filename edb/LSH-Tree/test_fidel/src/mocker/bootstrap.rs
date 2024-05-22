// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use ekvproto::fidel_timeshare::*;
use fidel_client::FidelMocker;



use super::*;

#[derive(Debug)]
pub struct AlreadyBootstrapped;

impl FidelMocker for AlreadyBootstrapped {
    fn bootstrap(&self, _: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        let mut err = Error::default();
        err.set_type(ErrorType::AlreadyBootstrapped);
        err.set_message("cluster is already bootstrapped".to_owned());

        let mut header = ResponseHeader::default();
        header.set_error(err);
        header.set_cluster_id(DEFAULT_CLUSTER_ID);

        let mut resp = BootstrapResponse::default();
        resp.set_header(header);

        Some(Ok(resp))
    }

    fn is_bootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        let mut header = ResponseHeader::default();
        header.set_cluster_id(DEFAULT_CLUSTER_ID);

        let mut resp = IsBootstrappedResponse::default();
        resp.set_bootstrapped(false);

        Some(Ok(resp))
    }
}

#[derive(Debug)]
pub struct Bootstrapped;

impl FidelMocker for Bootstrapped {
    fn is_bootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        let mut header = ResponseHeader::default();
        header.set_cluster_id(DEFAULT_CLUSTER_ID);

        let mut resp = IsBootstrappedResponse::default();
        resp.set_bootstrapped(true);

        Some(Ok(resp))
    }
}


#[derive(Debug)]
pub struct ChangeLeader {
    pub leader: Member,
}


