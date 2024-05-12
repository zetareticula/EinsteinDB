// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::{future, SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use grpcio::{
    DuplexSink, EnvBuilder, RequestStream, Result as GrpcResult, RpcContext, RpcStatus,
    RpcStatusCode, Server as GrpcServer, ServerBuilder, UnarySink, WriteFlags,
};
use fidel_client::Error as FidelError;
use security::*;

use ekvproto::fidel_timeshare::*;

use super::mocker::*;

pub struct Server<C: FidelMocker> {
    server: Option<GrpcServer>,
    mocker: FidelMock<C>,
}

impl Server<Service> {
    pub fn new(eps_count: usize) -> Server<Service> {
        let mgr = SecurityManager::new(&SecurityConfig::default()).unwrap();
        let eps = vec![("127.0.0.1".to_owned(), 0); eps_count];
        let case = Option::None::<Arc<Service>>;
        Self::with_configuration(&mgr, eps, case)
    }

    pub fn default_handler(&self) -> &Service {
        &self.mocker.default_handler
    }
}

impl<C: FidelMocker + lightlike + Sync + 'static> Server<C> {
    pub fn with_case(eps_count: usize, case: Arc<C>) -> Server<C> {
        let mgr = SecurityManager::new(&SecurityConfig::default()).unwrap();
        let eps = vec![("127.0.0.1".to_owned(), 0); eps_count];
        Server::with_configuration(&mgr, eps, Some(case))
    }

    pub fn with_configuration(
        mgr: &SecurityManager,
        eps: Vec<(String, u16)>,
        case: Option<Arc<C>>,
    ) -> Server<C> {
        let handler = Arc::new(Service::new());
        let default_handler = Arc::clone(&handler);
        let mocker = FidelMock {
            default_handler,
            case,
        };
        let mut server = Server {
            server: None,
            mocker,
        };
        server.spacelike(mgr, eps);
        server
    }

    pub fn spacelike(&mut self, mgr: &SecurityManager, eps: Vec<(String, u16)>) {
        let service = create_fidel(self.mocker.clone());
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)
                .name_prefix(thd_name!("mock-server"))
                .build(),
        );
        let mut sb = ServerBuilder::new(env).register_service(service);
        for (host, port) in eps {
            sb = mgr.bind(sb, &host, port);
        }

        let mut server = sb.build().unwrap();
        {
            let addrs: Vec<String> = server
                .bind_addrs()
                .map(|(host, port)| format!("{}:{}", host, port))
                .collect();
            self.mocker.default_handler.set_lightlikepoints(addrs.clone());
            if let Some(case) = self.mocker.case.as_ref() {
                case.set_lightlikepoints(addrs);
            }
        }

        server.spacelike();
        self.server = Some(server);
        // Ensure that server is ready.
        thread::sleep(Duration::from_secs(1));
    }

    pub fn stop(&mut self) {
        self.server
            .take()
            .expect("Server is not spacelikeed")
            .shutdown();
    }

    pub fn bind_addrs(&self) -> Vec<(String, u16)> {
        self.server
            .as_ref()
            .unwrap()
            .bind_addrs()
            .map(|(host, port)| (host.clone(), port))
            .collect()
    }
}

fn hijack_unary<F, R, C: FidelMocker>(
    mock: &mut FidelMock<C>,
    ctx: RpcContext<'_>,
    sink: UnarySink<R>,
    f: F,
) where
    R: lightlike + 'static,
    F: Fn(&dyn FidelMocker) -> Option<Result<R>>,
{
    let resp = mock
        .case
        .as_ref()
        .and_then(|case| f(case.as_ref()))
        .or_else(|| f(mock.default_handler.as_ref()));

    match resp {
        Some(Ok(resp)) => ctx.spawn(
            sink.success(resp)
                .unwrap_or_else(|e| error!("failed to reply: {:?}", e)),
        ),
        Some(Err(err)) => {
            let status = RpcStatus::new(RpcStatusCode::UNKNOWN, Some(format!("{:?}", err)));
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("failed to reply: {:?}", e)),
            );
        }
        _ => {
            let status = RpcStatus::new(
                RpcStatusCode::UNIMPLEMENTED,
                Some("Unimplemented".to_owned()),
            );
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("failed to reply: {:?}", e)),
            );
        }
    }
}

#[derive(Debug)]
struct FidelMock<C: FidelMocker> {
    default_handler: Arc<Service>,
    case: Option<Arc<C>>,
}

impl<C: FidelMocker> Clone for FidelMock<C> {
    fn clone(&self) -> Self {
        FidelMock {
            default_handler: Arc::clone(&self.default_handler),
            case: self.case.clone(),
        }
    }
}

impl<C: FidelMocker + lightlike + Sync + 'static> Fidel for FidelMock<C> {
    fn get_members(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetMembersRequest,
        sink: UnarySink<GetMembersResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_members(&req))
    }

    fn tso(
        &mut self,
        ctx: RpcContext<'_>,
        req: RequestStream<TsoRequest>,
        mut resp: DuplexSink<TsoResponse>,
    ) {
        let header = Service::header();
        let fut = async move {
            resp.lightlike_all(&mut req.map(move |_| {
                let mut r = TsoResponse::default();
                r.set_header(header.clone());
                r.mut_timestamp().physical = 42;
                GrpcResult::Ok((r, WriteFlags::default()))
            }))
            .await
            .unwrap();
            resp.close().await.unwrap();
        };
        ctx.spawn(fut);
    }

    fn bootstrap(
        &mut self,
        ctx: RpcContext<'_>,
        req: BootstrapRequest,
        sink: UnarySink<BootstrapResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.bootstrap(&req))
    }

    fn is_bootstrapped(
        &mut self,
        ctx: RpcContext<'_>,
        req: IsBootstrappedRequest,
        sink: UnarySink<IsBootstrappedResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.is_bootstrapped(&req))
    }

    fn alloc_id(
        &mut self,
        ctx: RpcContext<'_>,
        req: AllocIdRequest,
        sink: UnarySink<AllocIdResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.alloc_id(&req))
    }

    fn get_store(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetStoreRequest,
        sink: UnarySink<GetStoreResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_store(&req))
    }

    fn put_store(
        &mut self,
        ctx: RpcContext<'_>,
        req: PutStoreRequest,
        sink: UnarySink<PutStoreResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.put_store(&req))
    }

    fn get_all_stores(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetAllStoresRequest,
        sink: UnarySink<GetAllStoresResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_all_stores(&req))
    }

    fn store_heartbeat(
        &mut self,
        ctx: RpcContext<'_>,
        req: StoreHeartbeatRequest,
        sink: UnarySink<StoreHeartbeatResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.store_heartbeat(&req))
    }

    fn brane_heartbeat(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BraneHeartbeatRequest>,
        sink: DuplexSink<BraneHeartbeatResponse>,
    ) {
        let mock = self.clone();
        ctx.spawn(async move {
            let mut stream = stream.map_err(FidelError::from).try_filter_map(move |req| {
                let resp = mock
                    .case
                    .as_ref()
                    .and_then(|case| case.brane_heartbeat(&req))
                    .or_else(|| mock.default_handler.brane_heartbeat(&req));
                match resp {
                    None => future::ok(None),
                    Some(Ok(resp)) => future::ok(Some((resp, WriteFlags::default()))),
                    Some(Err(e)) => future::err(box_err!("{:?}", e)),
                }
            });
            let mut sink = sink.sink_map_err(FidelError::from);
            sink.lightlike_all(&mut stream).await.unwrap();
            let _ = sink.close().await;
        });
    }

    fn get_brane(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetBraneRequest,
        sink: UnarySink<GetBraneResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_brane(&req))
    }

    fn get_brane_by_id(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetBraneByIdRequest,
        sink: UnarySink<GetBraneResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_brane_by_id(&req))
    }

    fn ask_split(
        &mut self,
        ctx: RpcContext<'_>,
        req: AskSplitRequest,
        sink: UnarySink<AskSplitResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.ask_split(&req))
    }

    fn report_split(
        &mut self,
        _: RpcContext<'_>,
        _: ReportSplitRequest,
        _: UnarySink<ReportSplitResponse>,
    ) {
        unimplemented!()
    }

    fn ask_batch_split(
        &mut self,
        ctx: RpcContext<'_>,
        req: AskBatchSplitRequest,
        sink: UnarySink<AskBatchSplitResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.ask_batch_split(&req))
    }

    fn report_batch_split(
        &mut self,
        ctx: RpcContext<'_>,
        req: ReportBatchSplitRequest,
        sink: UnarySink<ReportBatchSplitResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.report_batch_split(&req))
    }

    fn get_cluster_config(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetClusterConfigRequest,
        sink: UnarySink<GetClusterConfigResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_cluster_config(&req))
    }

    fn put_cluster_config(
        &mut self,
        ctx: RpcContext<'_>,
        req: PutClusterConfigRequest,
        sink: UnarySink<PutClusterConfigResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.put_cluster_config(&req))
    }

    fn scatter_brane(
        &mut self,
        ctx: RpcContext<'_>,
        req: ScatterBraneRequest,
        sink: UnarySink<ScatterBraneResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.scatter_brane(&req))
    }

    fn get_prev_brane(
        &mut self,
        _: RpcContext<'_>,
        _: GetBraneRequest,
        _: UnarySink<GetBraneResponse>,
    ) {
        unimplemented!()
    }

    fn get_gc_safe_point(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetGcSafePointRequest,
        sink: UnarySink<GetGcSafePointResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_gc_safe_point(&req))
    }

    fn fidelio_gc_safe_point(
        &mut self,
        ctx: RpcContext<'_>,
        req: fidelioGcSafePointRequest,
        sink: UnarySink<fidelioGcSafePointResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.fidelio_gc_safe_point(&req))
    }

    fn sync_branes(
        &mut self,
        _ctx: RpcContext<'_>,
        _stream: RequestStream<SyncBraneRequest>,
        _sink: DuplexSink<SyncBraneResponse>,
    ) {
        unimplemented!()
    }

    fn get_operator(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetOperatorRequest,
        sink: UnarySink<GetOperatorResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_operator(&req))
    }

    fn scan_branes(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: ScanBranesRequest,
        _sink: UnarySink<ScanBranesResponse>,
    ) {
        unimplemented!()
    }

    fn fidelio_service_gc_safe_point(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: fidelioServiceGcSafePointRequest,
        _sink: UnarySink<fidelioServiceGcSafePointResponse>,
    ) {
        unimplemented!()
    }
}
