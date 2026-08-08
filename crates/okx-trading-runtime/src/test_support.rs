use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

#[derive(Clone, Default)]
pub(crate) struct CapturedLogs {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    pub(crate) fn dispatch(&self) -> tracing::Dispatch {
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(self.clone());
        tracing::Dispatch::new(tracing_subscriber::registry().with(layer))
    }

    pub(crate) fn contents(&self) -> String {
        let buffer = self.buffer.lock().expect("captured log buffer poisoned");
        String::from_utf8_lossy(&buffer).to_string()
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedLogWriter {
            buffer: self.buffer.clone(),
        }
    }
}

pub(crate) struct CapturedLogWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for CapturedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer
            .lock()
            .map_err(|_| io::Error::other("captured log buffer poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) struct HttpTestServer {
    addr: SocketAddr,
    requests: Option<tokio::task::JoinHandle<anyhow::Result<Vec<String>>>>,
}

impl HttpTestServer {
    pub(crate) async fn spawn(responses: Vec<String>) -> anyhow::Result<Self> {
        Self::spawn_inner(ok_responses(responses), None).await
    }

    pub(crate) async fn spawn_with_status(responses: Vec<(u16, String)>) -> anyhow::Result<Self> {
        Self::spawn_inner(responses, None).await
    }

    pub(crate) async fn spawn_with_response_delay(
        responses: Vec<String>,
        response_delay: Duration,
    ) -> anyhow::Result<Self> {
        Self::spawn_inner(ok_responses(responses), Some(response_delay)).await
    }

    async fn spawn_inner(
        responses: Vec<(u16, String)>,
        response_delay: Option<Duration>,
    ) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let requests = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let mut stream = accept_test_http_connection(&listener).await?;
                requests.push(read_http_request(&mut stream).await?);
                if let Some(response_delay) = response_delay {
                    time::sleep(response_delay).await;
                }
                let reason = match status {
                    200 => "OK",
                    429 => "Too Many Requests",
                    500 => "Internal Server Error",
                    _ => "Error",
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let write_result = write_test_http_response(&mut stream, response.as_bytes()).await;
                if response_delay.is_none() {
                    write_result?;
                }
            }
            Ok(requests)
        });
        Ok(Self {
            addr,
            requests: Some(requests),
        })
    }

    pub(crate) const fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) async fn await_requests(mut self) -> anyhow::Result<Vec<String>> {
        let mut requests = self
            .requests
            .take()
            .context("test HTTP server requests already awaited")?;
        let join_result = match time::timeout(TEST_HTTP_JOIN_TIMEOUT, &mut requests).await {
            Ok(join_result) => join_result,
            Err(error) => {
                requests.abort();
                let _ = requests.await;
                return Err(error).context("timed out waiting for test HTTP server task");
            }
        };
        join_result.context("test HTTP server task panicked")?
    }
}

const TEST_HTTP_IO_TIMEOUT: Duration = Duration::from_secs(1);
const TEST_HTTP_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

fn ok_responses(responses: Vec<String>) -> Vec<(u16, String)> {
    responses
        .into_iter()
        .map(|body| (200, body))
        .collect::<Vec<_>>()
}

async fn read_http_request(stream: &mut TcpStream) -> anyhow::Result<String> {
    time::timeout(TEST_HTTP_IO_TIMEOUT, read_http_request_inner(stream))
        .await
        .context("timed out reading test HTTP request")?
}

async fn read_http_request_inner(stream: &mut TcpStream) -> anyhow::Result<String> {
    let mut request = Vec::new();
    let mut header_end = None;
    loop {
        let mut buffer = [0; 1024];
        let bytes_read = stream.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..bytes_read]);
        if header_end.is_none() {
            header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4);
        }
        let Some(header_end) = header_end else {
            continue;
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if request.len() >= header_end + content_length {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&request).to_string())
}

async fn accept_test_http_connection(listener: &TcpListener) -> anyhow::Result<TcpStream> {
    let (stream, _) = time::timeout(TEST_HTTP_IO_TIMEOUT, listener.accept())
        .await
        .context("timed out accepting test HTTP connection")??;
    Ok(stream)
}

async fn write_test_http_response(stream: &mut TcpStream, response: &[u8]) -> io::Result<()> {
    time::timeout(TEST_HTTP_IO_TIMEOUT, stream.write_all(response))
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out writing test HTTP response",
            )
        })?
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tokio::{io::AsyncWriteExt, net::TcpStream};

    use super::HttpTestServer;

    #[tokio::test]
    async fn http_test_server_times_out_when_no_client_connects() -> Result<()> {
        let server = HttpTestServer::spawn(vec![r#"{"code":"0","data":[]}"#.to_owned()]).await?;

        let error = server
            .await_requests()
            .await
            .expect_err("missing test client should time out");

        assert!(
            format!("{error:#}").contains("timed out accepting test HTTP connection"),
            "unexpected timeout error: {error:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_test_server_times_out_on_incomplete_request() -> Result<()> {
        let server = HttpTestServer::spawn(vec![r#"{"code":"0","data":[]}"#.to_owned()]).await?;
        let mut stream = TcpStream::connect(server.addr()).await?;
        stream
            .write_all(b"GET /api/v5/market/ticker HTTP/1.1\r\n")
            .await?;

        let error = server
            .await_requests()
            .await
            .expect_err("incomplete test request should time out");

        assert!(
            format!("{error:#}").contains("timed out reading test HTTP request"),
            "unexpected timeout error: {error:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_test_server_join_times_out_on_slow_server_task() -> Result<()> {
        let server = HttpTestServer::spawn_with_response_delay(
            vec![r#"{"code":"0","data":[]}"#.to_owned()],
            super::TEST_HTTP_JOIN_TIMEOUT + std::time::Duration::from_secs(1),
        )
        .await?;
        let mut stream = TcpStream::connect(server.addr()).await?;
        stream
            .write_all(b"GET /api/v5/market/ticker HTTP/1.1\r\nHost: local\r\n\r\n")
            .await?;

        let error = server
            .await_requests()
            .await
            .expect_err("slow server task should time out");

        assert!(
            format!("{error:#}").contains("timed out waiting for test HTTP server task"),
            "unexpected timeout error: {error:#}"
        );
        Ok(())
    }
}
