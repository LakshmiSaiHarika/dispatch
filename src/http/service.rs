use std::future::Future;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{stream, StreamExt};
use http_body_util::{combinators::BoxBody, BodyExt, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::{Method, StatusCode as Code};
use hyper::{Request, Response};
use reqwest::Client;
use std::convert::Infallible;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use zip::ZipArchive;

use crate::github::{Asset, GitHub, Report, Type};
use crate::tui::Status;

// This contains the bytes of the poweroff.efi module.
const POWEROFF_EFI: &[u8] = include_bytes!(env!("POWEROFF_BIN_PATH"));
const EMPTY: &[u8] = &[];

/// Main HTTP service that handles all requests
pub struct Service {
    remote: IpAddr,
    status: Arc<Mutex<Status>>,
    github: Arc<GitHub>,
    client: Client,
    path: Arc<String>,
    upload_dir: Arc<PathBuf>,
    auto_extract: bool,
    allow_anon_upload: bool,
}

impl Service {
    pub fn new(
        remote: IpAddr,
        status: Arc<Mutex<Status>>,
        github: Arc<GitHub>,
        client: Client,
        path: Arc<String>,
        upload_dir: Arc<PathBuf>,
        auto_extract: bool,
        allow_anon_upload: bool,
    ) -> Self {
        Self {
            remote,
            status,
            github,
            client,
            path,
            upload_dir,
            auto_extract,
            allow_anon_upload,
        }
    }

    /// Build a reqwest request for an asset, handling private/public repos and method.
    fn build_request(
        gh: &GitHub,
        asset: &Asset,
        client: &Client,
        method: &Method,
    ) -> reqwest::RequestBuilder {
        let url = if gh.is_private() {
            format!(
                "https://api.github.com/repos/{}/{}/releases/assets/{}",
                gh.owner(),
                gh.repo(),
                asset.id
            )
        } else {
            asset.url.clone()
        };

        match method {
            &Method::HEAD => client.head(url),
            // Only HEAD and GET are expected; all others default to GET.
            _ => client.get(url),
        }
    }
}

impl hyper::service::Service<Request<Incoming>> for Service {
    type Response = Response<BoxBody<Bytes, Infallible>>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let status = self.status.clone();
        let client = self.client.clone();
        let github = self.github.clone();
        let path = self.path.clone();
        let remote = self.remote;
        let upload_dir = self.upload_dir.clone();
        let auto_extract = self.auto_extract;
        let allow_anon_upload = self.allow_anon_upload;

        Box::pin(async move {
            if req.uri().path() != *path {
                return Ok(EMPTY.reply(Code::NOT_FOUND, None, None));
            }

            let (response, ct) = match *req.method() {
                // The POST request is used to signal the start of a job.
                Method::POST => {
                    if status.lock().await.update().booting(remote) {
                        return Ok(EMPTY.reply(None, None, None));
                    }

                    return Ok(EMPTY.reply(Code::EXPECTATION_FAILED, None, None));
                }

                // The PUT request is used to report completion of a job.
                Method::PUT => {
                    // Collect the request body
                    let bytes = req.into_body().collect().await?.to_bytes();
                    let report: Report = match serde_json::from_slice(&bytes) {
                        Err(..) => return Ok(EMPTY.reply(Code::BAD_REQUEST, None, None)),
                        Ok(report) => report,
                    };

                    // Display that the job has been reported.
                    if !status.lock().await.update().report(remote) {
                        return Ok(EMPTY.reply(Code::EXPECTATION_FAILED, None, None));
                    }

                    // Create a GitHub issue for the report.
                    let reported = tokio::time::Instant::now();
                    if github.report(report).await.is_err() {
                        return Ok(EMPTY.reply(Code::INTERNAL_SERVER_ERROR, None, None));
                    }

                    // Mark the job as finished.
                    tokio::spawn(async move {
                        tokio::time::sleep_until(reported + Duration::from_secs(5)).await;
                        status.lock().await.update().finish(remote);
                    });

                    return Ok(EMPTY.reply(None, None, None));
                }

                // The PATCH request is used to upload files.
                Method::PATCH => {
                    // Verify the client has an active job (unless anonymous uploads are allowed)
                    if !allow_anon_upload && !status.lock().await.update().is_active(remote) {
                        return Ok(EMPTY.reply(Code::EXPECTATION_FAILED, None, None));
                    }

                    // Get archive name from header or generate default
                    let archive_name = req
                        .headers()
                        .get("x-archive-name")
                        .and_then(|v| v.to_str().ok())
                        .map(sanitize_filename)
                        .unwrap_or_else(|| {
                            format!("upload-{}-{}.zip", remote, chrono_timestamp())
                        });

                    // Collect the request body
                    let bytes = req.into_body().collect().await?.to_bytes();
                    if bytes.is_empty() {
                        return Ok(EMPTY.reply(Code::BAD_REQUEST, None, None));
                    }

                    // Create subdirectory for this remote IP
                    let remote_dir = upload_dir.join(remote.to_string());
                    if let Err(e) = tokio::fs::create_dir_all(&remote_dir).await {
                        eprintln!("Failed to create upload directory: {e}");
                        return Ok(EMPTY.reply(Code::INTERNAL_SERVER_ERROR, None, None));
                    }

                    // Write the file
                    let file_path = remote_dir.join(&archive_name);
                    match File::create(&file_path).await {
                        Ok(mut file) => {
                            if let Err(e) = file.write_all(&bytes).await {
                                eprintln!("Failed to write upload: {e}");
                                return Ok(EMPTY.reply(Code::INTERNAL_SERVER_ERROR, None, None));
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to create upload file: {e}");
                            return Ok(EMPTY.reply(Code::INTERNAL_SERVER_ERROR, None, None));
                        }
                    }

                    // Auto-extract if enabled
                    if auto_extract {
                        let extract_dir = remote_dir.join(
                            archive_name
                                .strip_suffix(".zip")
                                .unwrap_or(&archive_name)
                        );
                        if let Err(e) = extract_zip(&file_path, &extract_dir).await {
                            eprintln!("Failed to extract upload: {e}");
                            // Don't fail the request, just log the error
                        }
                    }

                    return Ok(EMPTY.reply(Code::CREATED, None, None));
                }

                // The HEAD request is used to get information about the assigned asset.
                Method::HEAD => {
                    match status.clone().assign(remote).await {
                        // No asset assigned, return poweroff EFI binary.
                        None => return Ok(POWEROFF_EFI.reply(None, Type::Efi, EMPTY)),

                        // Send the request (possibly redirecting...)
                        Some(asset) => {
                            let request =
                                Self::build_request(&github, &asset, &client, &Method::HEAD);

                            (request.send().await?, asset.mime)
                        }
                    }
                }

                // The GET request is used to fetch the assigned asset.
                Method::GET => {
                    match status.clone().assign(remote).await {
                        // No asset assigned, return poweroff EFI binary.
                        None => return Ok(POWEROFF_EFI.reply(None, Type::Efi, None)),

                        // Send the request (possibly redirecting...)
                        Some(asset) => {
                            let request =
                                Self::build_request(&github, &asset, &client, &Method::GET);

                            let response = request.send().await?;
                            status.lock().await.update().downloading(remote);
                            (response, asset.mime)
                        }
                    }
                }

                // Bad method.
                _ => {
                    return Ok(Response::builder()
                        .status(Code::METHOD_NOT_ALLOWED)
                        .header("allow", "GET, POST, HEAD, PUT, PATCH")
                        .body(EMPTY.embody())?)
                }
            };

            let content_type = ct.content_type().parse().unwrap();

            // Construct the response.
            let mut builder = Response::builder().status(response.status());
            for (key, mut value) in response.headers() {
                // GitHub always returns `application/octet-stream` for EFI
                // binaries, so we override it here.
                if key == "content-type" {
                    value = &content_type;
                }

                builder = builder.header(key, value);
            }

            // Stream the response body directly, mapping errors to Infallible
            Ok(builder.body(BoxBody::new(StreamBody::new(Box::pin(
                response.bytes_stream().map(|result| {
                    result.map_or_else(
                        |_| Ok(Frame::data(Bytes::new())),
                        |bytes| Ok(Frame::data(bytes)),
                    )
                }),
            ))))?)
        })
    }
}

trait Embody {
    fn embody(self) -> BoxBody<Bytes, Infallible>;
}

impl Embody for &'static [u8] {
    fn embody(self) -> BoxBody<Bytes, Infallible> {
        BoxBody::new(StreamBody::new(Box::pin(stream::once(async move {
            Ok(Frame::data(Bytes::from(self)))
        }))))
    }
}

trait Reply {
    fn reply(
        self,
        code: impl Into<Option<Code>>,
        ct: impl Into<Option<Type>>,
        body: impl Into<Option<&'static [u8]>>,
    ) -> Response<BoxBody<Bytes, Infallible>>;
}

impl Reply for &'static [u8] {
    fn reply(
        self,
        code: impl Into<Option<Code>>,
        ct: impl Into<Option<Type>>,
        body: impl Into<Option<&'static [u8]>>,
    ) -> Response<BoxBody<Bytes, Infallible>> {
        let mut builder = Response::builder()
            .status(code.into().unwrap_or(Code::OK))
            .header("content-length", self.len());

        if let Some(ct) = ct.into() {
            builder = builder.header("content-type", ct.content_type());
        }

        builder.body(body.into().unwrap_or(self).embody()).unwrap()
    }
}

trait Assign {
    async fn assign(self, ip: IpAddr) -> Option<Asset>;
}

impl Assign for Arc<Mutex<Status>> {
    async fn assign(self, ip: IpAddr) -> Option<Asset> {
        self.lock().await.update().assign(ip)
    }
}

/// Sanitizes a filename to prevent path traversal attacks.
fn sanitize_filename(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric() || *c == '.' || *c == '-' || *c == '_')
        .collect::<String>()
        .trim_start_matches('.')
        .to_string()
}

/// Generates a timestamp string for default filenames.
fn chrono_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Extracts a ZIP archive to a specified directory.
async fn extract_zip(zip_path: &Path, extract_to: &Path) -> Result<(), String> {
    let zip_path = zip_path.to_path_buf();
    let extract_to = extract_to.to_path_buf();

    // Run extraction in a blocking task since zip crate is not async
    tokio::task::spawn_blocking(move || -> Result<(), String> {
        let file = std::fs::File::open(&zip_path)
            .map_err(|e| format!("Failed to open zip: {e}"))?;
        let mut archive = ZipArchive::new(file)
            .map_err(|e| format!("Failed to read zip archive: {e}"))?;

        for i in 0..archive.len() {
            let mut file = archive.by_index(i)
                .map_err(|e| format!("Failed to read zip entry {i}: {e}"))?;
            let outpath = extract_to.join(file.name());

            // Prevent path traversal attacks
            if !outpath.starts_with(&extract_to) {
                eprintln!("Warning: Skipping file with invalid path: {}", file.name());
                continue;
            }

            if file.is_dir() {
                std::fs::create_dir_all(&outpath)
                    .map_err(|e| format!("Failed to create directory: {e}"))?;
            } else {
                if let Some(p) = outpath.parent() {
                    std::fs::create_dir_all(p)
                        .map_err(|e| format!("Failed to create parent directory: {e}"))?;
                }
                let mut outfile = std::fs::File::create(&outpath)
                    .map_err(|e| format!("Failed to create file: {e}"))?;
                std::io::copy(&mut file, &mut outfile)
                    .map_err(|e| format!("Failed to write file: {e}"))?;
            }
        }

        Ok(())
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))??;

    Ok(())
}
