//! This is a tool that uses the synapse_compress_state library to
//! reduce the size of the synapse state_groups_state table in a postgres
//! database.
//!
//! It adds the tables state_compressor_state and state_compressor_progress
//! to the database and uses these to enable it to incrementally work
//! on space reductions

use std::str::FromStr;

use log::LevelFilter;
use pyo3::{
    exceptions::PyRuntimeError, prelude::pymodule, types::PyModule, PyErr, PyResult, Python,
};
use pyo3_log::Logger;

pub mod manager;
pub mod state_saving;

/// Type alias for one section of the compressor state (used when saving and restoring)
///
/// The compressor keeps track of a number of Levels, each of which have a maximum length,
/// current length, and an optional current head (None if level is empty, Some if a head
/// exists).
///
/// The LevelState type is used as the triple (max_length, current_length, current_head)
pub type LevelState = (usize, usize, Option<i64>);

/// Helper struct for parsing the `default_levels` argument.
// This is needed since FromStr cannot be implemented for structs
// that aren't defined in this scope
#[derive(PartialEq, Debug)]
pub struct LevelInfo(pub Vec<LevelState>);

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

// PyO3 INTERFACE STARTS HERE
#[pymodule]
// #[pyo3(name = "state_compressor")]
fn auto_compressor(_py: Python, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, compress_largest_rooms)]
    fn compress_largest_rooms(
        _py: Python,
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
            log::error!("{}",e);
            return Err(PyErr::new::<PyRuntimeError, _>(e.to_string()));
        }

        log::info!("auto_compressor finished");
        Ok(())
    }
    Ok(())
}
