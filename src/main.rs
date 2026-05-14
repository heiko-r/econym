use anyhow::Result;
use clap::{Parser, Subcommand};
use std::time::Instant;

use crate::{geocoder::ReverseGeocoder, webserver::serve};
mod geocoder;
mod webserver;

#[derive(Parser)]
#[command(name = "econym")]
#[command(about = "Minimal reverse geocoder using Overture Maps data", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Load Overture data and save to file
    Load {
        /// Path to Overture GeoParquet file
        #[arg(short, long)]
        input: String,
        /// Path to save the geocoder file
        #[arg(short, long, default_value = "geocoder.rkyv")]
        output: String,
    },
    /// Lookup nearest place for given coordinates
    Lookup {
        /// Latitude
        #[arg(long)]
        lat: f64,
        /// Longitude
        #[arg(long)]
        lon: f64,
        /// Path to the geocoder file
        #[arg(short, long, default_value = "geocoder.rkyv")]
        input: String,
        /// In-memory mode
        #[arg(long, default_value_t = false)]
        in_memory: bool,
    },
    /// Start web server for reverse geocoding API
    Serve {
        /// Port number to listen on
        #[arg(short, long, default_value = "3000")]
        port: u16,
        /// Path to the geocoder file
        #[arg(short, long, default_value = "geocoder.rkyv")]
        input: String,
        /// In-memory mode
        #[arg(long, default_value_t = false)]
        in_memory: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Load { input, output } => {
            println!("Loading Overture data from: {}", input);
            let before = Instant::now();
            let mut geocoder = ReverseGeocoder::new();
            geocoder.load_from_overture(&input)?;
            geocoder.save_to_file(&output)?;
            println!("Saved to: {}", output);
            println!("Total time: {:.2?}", before.elapsed());
        }
        Commands::Lookup {
            lat,
            lon,
            input,
            in_memory,
        } => {
            println!("Loading geocoder from: {}", input);
            let before = Instant::now();
            let mut geocoder = ReverseGeocoder::new();
            if in_memory {
                geocoder.load_from_file(&input)?;
            } else {
                geocoder.zero_copy_from_file(&input)?;
            }
            println!("Loaded after: {:.2?}", before.elapsed());

            if let Some(place) = geocoder.nearest_place(lat, lon) {
                println!(
                    "Nearest place: {} ({}, {})",
                    place.name, place.latitude, place.longitude
                );
            } else {
                println!("No place found");
            }

            println!("Elapsed time: {:.2?}", before.elapsed());
        }
        Commands::Serve {
            port,
            input,
            in_memory,
        } => {
            tokio::runtime::Runtime::new()?
                .block_on(async { serve(port, input, in_memory).await })?;
        }
    }

    Ok(())
}
