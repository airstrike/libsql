use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::{Extension, Router};
use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tonic::transport::server::TcpConnectInfo;

use crate::net::AddrIncoming;

#[tokio::test]
async fn upgrades_http1_to_http2_and_preserves_the_peer_address() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = Arc::new(Notify::new());
    let router = Router::new().route(
        "/",
        get(|Extension(info): Extension<TcpConnectInfo>| async move {
            info.remote_addr().unwrap().to_string()
        }),
    );
    let server = tokio::spawn(super::serve(
        AddrIncoming::new(listener),
        router,
        shutdown.clone(),
        true,
    ));

    tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let peer = stream.local_addr().unwrap();
        stream
            .write_all(
                b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABk\r\n\r\n",
            )
            .await
            .unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            header.push(stream.read_u8().await.unwrap());
            assert!(header.len() < 1024);
        }
        assert!(header.starts_with(b"HTTP/1.1 101"));

        let (mut sender, connection) = hyper::client::conn::http2::handshake(
            TokioExecutor::new(),
            TokioIo::new(stream),
        )
        .await
        .unwrap();
        let client = tokio::spawn(connection);
        let request = Request::builder()
            .uri(format!("http://{address}/"))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), peer.to_string().as_bytes());
        client.abort();
    })
    .await
    .unwrap();

    shutdown.notify_one();
    server.await.unwrap();
}
