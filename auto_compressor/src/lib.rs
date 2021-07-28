//! This is a tool that uses the synapse_compress_state library to 
//! reduce the size of the synapse state_groups_state table.
//!
//! It adds the tables state_compressor_state and state_compressor_progress
//! to the database and uses these to enable it to incrementally work
//! on space reductions

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
