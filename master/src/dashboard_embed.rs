//! Static dashboard baked in at compile time (`embed-dashboard` feature).

use axum::{
    body::Body,
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../dashboard/dist/"]
struct DashboardAssets;

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "application/javascript",
        "css" => "text/css",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "json" => "application/json",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "txt" => "text/plain",
        _ => "application/octet-stream",
    }
}

fn response_for(path: &str) -> Option<Response> {
    let file = DashboardAssets::get(path)?;
    let mut res = Response::new(Body::from(file.data.into_owned()));
    *res.status_mut() = StatusCode::OK;
    if let Ok(value) = header::HeaderValue::from_str(content_type(path)) {
        res.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    Some(res)
}

pub async fn serve(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    if let Some(res) = response_for(path) {
        return res;
    }

    response_for("index.html").unwrap_or_else(|| StatusCode::NOT_FOUND.into_response())
}

pub fn has_index() -> bool {
    DashboardAssets::get("index.html").is_some()
}
