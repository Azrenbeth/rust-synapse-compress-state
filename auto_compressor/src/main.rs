//! This is a tool that uses the synapse_compress_state library to
//! reduce the size of the synapse state_groups_state table in a postgres
//! database.
//!
//! It adds the tables state_compressor_state and state_compressor_progress
//! to the database and uses these to enable it to incrementally work
//! on space reductions.
//!
//! This binary calls manager::compress_largest_rooms() with the arguments
//! provided. That is, it compresses (in batches) the top N rooms ranked by
//! amount of "uncompressed" state. This is measured by the number of rows in
//! the state_groups_state table.
//!
//! After each batch, the rows processed are marked as "compressed" (using
//! the state_compressor_progress table), and the program state is saved into
//! the state_compressor_state table so that the compressor can seemlesly
//! continue from where it left off.

use auto_compressor::{manager, state_saving, LevelState};
use clap::{crate_authors, crate_description, crate_name, crate_version, value_t, App, Arg};
use log::LevelFilter;
use std::{env, str::FromStr};

/// Execution starts here
fn main() {
    if env::var("COMPRESSOR_LOG_LEVEL").is_err() {
        let mut log_builder = env_logger::builder();
        log_builder.filter_module("synapse_compress_state", LevelFilter::Warn);
        log_builder.filter_module("auto_compressor", LevelFilter::Info);
        log_builder.init();
    } else {
        env_logger::Builder::from_env("COMPRESSOR_LOG_LEVEL").init();
    }
    // parse the command line arguments using the clap crate
    let arguments = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .about(crate_description!())
        .arg(
            Arg::with_name("postgres-url")
                .short("p")
                .value_name("URL")
                .help("The url for connecting to the postgres database.")
                .long_help(concat!(
                    "The url for connecting to the postgres database.This should be of",
                    " the form \"postgresql://username:password@mydomain.com/database\""))
                .takes_value(true)
                .required(true),
        ).arg(
            Arg::with_name("chunk_size")
                .short("c")
                .value_name("COUNT")
                .help("The maximum number of state groups to load into memroy at once")
                .long_help(concat!(
                    "The number of state_groups to work on at once. All of the entries",
                    " from state_groups_state are requested from the database",
                    " for state groups that are worked on. Therefore small",
                    " chunk sizes may be needed on machines with low memory.",
                    " (Note: if the compressor fails to find space savings on the",
                    " chunk as a whole (which may well happen in rooms with lots",
                    " of backfill in) then the entire chunk is skipped.)",
                ))
                .takes_value(true)
                .required(true),
        ).arg(
            Arg::with_name("default_levels")
                .short("l")
                .value_name("LEVELS")
                .help("Sizes of each new level in the compression algorithm, as a comma separated list.")
                .long_help(concat!(
                    "Sizes of each new level in the compression algorithm, as a comma separated list.",
                    " The first entry in the list is for the lowest, most granular level,",
                    " with each subsequent entry being for the next highest level.",
                    " The number of entries in the list determines the number of levels",
                    " that will be used.",
                    "\nThe sum of the sizes of the levels effect the performance of fetching the state",
                    " from the database, as the sum of the sizes is the upper bound on number of",
                    " iterations needed to fetch a given set of state.",
                ))
                .default_value("100,50,25")
                .takes_value(true)
                .required(false),
        ).arg(
            Arg::with_name("number_of_rooms")
                .short("n")
                .value_name("ROOMS_TO_COMPRESS")
                .help("The number of rooms to compress") 
                .long_help("The top ROOMS_TO_COMPRESS rooms will be compressed ")
                .takes_value(true)
                .required(true),
        ).get_matches();

    // The URL of the database
    let db_url = arguments
        .value_of("postgres-url")
        .expect("A database url is required");

    // The number of state groups to work on at once
    let chunk_size = arguments
        .value_of("chunk_size")
        .map(|s| s.parse().expect("chunk_size must be an integer"))
        .expect("A chunk size is required");

    // The default structure to use when compressing
    let default_levels =
        value_t!(arguments, "default_levels", LevelInfo).unwrap_or_else(|e| panic!("Unable to parse default levels: {}", e));

    // The number of rooms to compress with this tool
    let number_of_rooms = arguments
        .value_of("number_of_rooms")
        .map(|s| s.parse().expect("number_of_rooms must be an integer"))
        .expect("number_of_rooms is required");

    // Connect to the database and create the 2 tables this tool needs
    // (Note: if they already exist then this does nothing)
    let mut client = state_saving::connect_to_database(db_url)
        .unwrap_or_else(|e| panic!("Error occured while connection to {}: {}", db_url, e));
    state_saving::create_tables_if_needed(&mut client)
        .unwrap_or_else(|e| panic!("Error occured while creating tables in database: {}", e));

    // call compress_largest_rooms with the arguments supplied
    manager::compress_largest_rooms(db_url, chunk_size, &default_levels.0, number_of_rooms);
}

/// Helper struct for parsing the `default_levels` argument.
// This is needed since FromStr cannot be implemented for structs
// that aren't defined in this scope
#[derive(PartialEq, Debug)]
struct LevelInfo(Vec<LevelState>);

// Implement FromStr so that an argument of the form "100,50,25"
// can be used to create LevelInfo<vec!((100,0,None),(50,0,None),(25,0,None))>
// For more info see the LevelState documentation in lib.rs
impl FromStr for LevelInfo {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Stores the max sizes of each level
        let mut level_info: Vec<LevelState> = Vec::new();

        // Split the string up at each comma
        for size_str in s.split(',') {
            // try and convert each section into a number
            // panic if that fails
            let size: usize = size_str
                .parse()
                .map_err(|_| "Not a comma separated list of numbers")?;
            // add this parsed number to the sizes struct
            level_info.push((size, 0, None));
        }

        // Return the built up vector inside a LevelInfo struct
        Ok(LevelInfo(level_info))
    }
}
