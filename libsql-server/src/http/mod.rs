pub mod admin;
pub mod user;

use std::future::poll_fn;
use std::sync::Arc;

use axum::body::Body;
use axum::Router;
use http::{header, HeaderValue, Request, Response, StatusCode, Version};
use hyper::body::Incoming;
use hyper::service::{service_fn, Service};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::sync::Notify;
use tower::ServiceExt;

use crate::net::{Accept, Conn};

async fn serve<A: Accept>(mut acceptor: A, router: Router, shutdown: Arc<Notify>, h2c: bool) {
    let builder = Builder::new(TokioExecutor::new());
    let graceful = GracefulShutdown::new();
    let shutdown = shutdown.notified();
    tokio::pin!(shutdown);

    loop {
        let connection = tokio::select! {
            biased;
            _ = &mut shutdown => break,
            connection = poll_fn(|cx| std::pin::Pin::new(&mut acceptor).poll_accept(cx)) => connection,
        };
        let connection = match connection {
            Some(Ok(connection)) => connection,
            Some(Err(error)) => {
                tracing::error!(%error, "could not accept an HTTP connection");
                continue;
            }
            None => break,
        };

        let router = router.clone();
        let connect_info = Conn::connect_info(&connection);
        let route = service_fn(move |mut request: Request<Incoming>| {
            request.extensions_mut().insert(connect_info.clone());
            router.clone().oneshot(request.map(Body::new))
        });
        let service = service_fn(move |mut request: Request<Incoming>| {
            let route = route.clone();
            async move {
                if h2c
                    && request.version() == Version::HTTP_11
                    && request.headers().get(header::UPGRADE)
                        == Some(&HeaderValue::from_static("h2c"))
                {
                    let upgrade = hyper::upgrade::on(&mut request);
                    tokio::spawn(async move {
                        let result = async {
                            let stream = upgrade.await?;
                            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                                .serve_connection(stream, route)
                                .await
                        }
                        .await;
                        if let Err(error) = result {
                            tracing::debug!(%error, "h2c connection ended");
                        }
                    });
                    let mut response = Response::new(Body::empty());
                    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
                    response
                        .headers_mut()
                        .insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
                    response
                        .headers_mut()
                        .insert(header::UPGRADE, HeaderValue::from_static("h2c"));
                    return Ok(response);
                }
                route.call(request).await
            }
        });
        let builder = builder.clone();
        let watcher = graceful.watcher();
        tokio::spawn(async move {
            let connection =
                builder.serve_connection_with_upgrades(TokioIo::new(connection), service);
            if let Err(error) = watcher.watch(connection).await {
                tracing::debug!(%error, "HTTP connection ended");
            }
        });
    }
    graceful.shutdown().await;
}

#[cfg(test)]
mod tests;
