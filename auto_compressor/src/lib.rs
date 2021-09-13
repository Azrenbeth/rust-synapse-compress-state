//! This is a tool that uses the synapse_compress_state library to
//! reduce the size of the synapse state_groups_state table in a postgres
//! database.
//!
//! It adds the tables state_compressor_state and state_compressor_progress
//! to the database and uses these to enable it to incrementally work
//! on space reductions

use std::str::FromStr;

use anyhow::Result;
use log::{error, LevelFilter};
use pyo3::{
    exceptions::PyRuntimeError, prelude::pymodule, types::PyModule, PyErr, PyResult, Python,
};
use pyo3_log::Logger;
use synapse_compress_state::Level;

pub mod manager;
pub mod state_saving;

/// Helper struct for parsing the `default_levels` argument.
///
/// The compressor keeps track of a number of Levels, each of which have a maximum length,
/// current length, and an optional current head (None if level is empty, Some if a head
/// exists).
///
/// This is needed since FromStr cannot be implemented for structs
/// that aren't defined in this scope
#[derive(PartialEq, Debug)]
pub struct LevelInfo(pub Vec<Level>);

// Implement FromStr so that an argument of the form "100,50,25"
// can be used to create a vector of levels with max sizes 100, 50 and 25
// For more info see the LevelState documentation in lib.rs
impl FromStr for LevelInfo {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Stores the max sizes of each level
        let mut level_info: Vec<Level> = Vec::new();

        // Split the string up at each comma
        for size_str in s.split(',') {
            // try and convert each section into a number
            // panic if that fails
            let size: usize = size_str
                .parse()
                .map_err(|_| "Not a comma separated list of numbers")?;
            // add this parsed number to the sizes struct
            level_info.push(Level::restore(size, 0, None));
        }

        // Return the built up vector inside a LevelInfo struct
        Ok(LevelInfo(level_info))
    }
}

// PyO3 INTERFACE STARTS HERE
#[pymodule]
// #[pyo3(name = "state_compressor")]
fn auto_compressor(_py: Python, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, compress_largest_rooms)]
    fn compress_largest_rooms(
        py: Python,
        db_url: String,
        chunk_size: i64,
        default_levels: String,
        number_of_rooms: i64,
    ) -> PyResult<()> {
        // Stops the compressor from holding the GIL while running
        py.allow_threads(|| {
            compress_largest_rooms_body(db_url, chunk_size, default_levels, number_of_rooms)
        })
    }

    fn compress_largest_rooms_body(
        db_url: String,
        chunk_size: i64,
        default_levels: String,
        number_of_rooms: i64,
    ) -> PyResult<()> {
        let _ = Logger::default()
            // don't send out anything lower than a warning from other crates
            .filter(LevelFilter::Warn)
            // don't log warnings from synapse_compress_state, the auto_compressor handles these
            // situations and provides better log messages
            .filter_target("synapse_compress_state".to_owned(), LevelFilter::Error)
            // log info and above for the auto_compressor
            .filter_target("auto_compressor".to_owned(), LevelFilter::Info)
            .install();
        // ensure any panics produce error messages in the log
        log_panics::init();
        // Announce the start of the program to the logs
        log::info!("auto_compressor started");
        // sleep(Duration::from_secs(10));

        // Parse the default_level string into a LevelInfo struct
        let default_levels: LevelInfo = match default_levels.parse() {
            Ok(l_sizes) => l_sizes,
            Err(e) => {
                return Err(PyErr::new::<PyRuntimeError, _>(format!(
                    "Unable to parse level_sizes: {}",
                    e
                )))
            }
        };

        // call compress_largest_rooms with the arguments supplied
        let run_result = manager::compress_largest_rooms(
            &db_url,
            chunk_size,
            &default_levels.0,
            number_of_rooms,
        );

        if let Err(e) = run_result {
            error!("{}", e);
            return Err(PyErr::new::<PyRuntimeError, _>(e.to_string()));
        }

        log::info!("auto_compressor finished");
        Ok(())
    }
    Ok(())
}
