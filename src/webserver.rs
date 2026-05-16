use anyhow::Result;
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

use crate::geocoder::ReverseGeocoder;

#[derive(SerdeDeserialize)]
struct LookupQuery {
    lat: f64,
    lon: f64,
}

#[derive(SerdeSerialize)]
struct LookupResponse {
    name: String,
}

async fn lookup_handler(
    Query(params): Query<LookupQuery>,
    State(geocoder): State<Arc<ReverseGeocoder>>,
) -> Result<Json<LookupResponse>, StatusCode> {
    if let Some(place) = geocoder.nearest_place(params.lat, params.lon) {
        Ok(Json(LookupResponse {
            name: place.name.clone(),
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn serve(port: u16, input: String, in_memory: bool) -> Result<()> {
    let mut geocoder = ReverseGeocoder::new();
    if in_memory {
        geocoder.load_from_file(&input)?;
    } else {
        geocoder.zero_copy_from_file(&input)?;
    }

    let geocoder = Arc::new(geocoder);

    let app = Router::new()
        .route("/lookup", get(lookup_handler))
        .layer(CorsLayer::permissive())
        .with_state(geocoder);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
