// This module contains functions that carry out diffferent types
// of compression on the database.

use crate::{
    state_saving::{
        connect_to_database, create_tables_if_needed, get_rooms_with_most_rows_to_compress,
        read_room_compressor_state, write_room_compressor_state,
    },
    LevelState,
};
use anyhow::{bail, Result};
use log::{debug, info, warn};
use synapse_compress_state::{continue_run, ChunkStats};

/// Runs the compressor on a chunk of the room
///
/// Returns `Some(chunk_stats)` if the compressor has progressed
/// and `None` if it had already got to the end of the room
///
/// # Arguments
///
/// * `db_url`          -   The URL of the postgres database that synapse is using.
///                         e.g. "postgresql://user:password@domain.com/synapse"
///
/// * `room_id`         -   The id of the room to run the compressor on. Note this
///                         is the id as stored in the database and will look like
///                         "!aasdfasdfafdsdsa:matrix.org" instead of the common
///                         name
///
/// * `chunk_size`      -   The number of state_groups to work on. All of the entries
///                         from state_groups_state are requested from the database
///                         for state groups that are worked on. Therefore small
///                         chunk sizes may be needed on machines with low memory.
///                         (Note: if the compressor fails to find space savings on the
///                         chunk as a whole (which may well happen in rooms with lots
///                         of backfill in) then the entire chunk is skipped.)
///
/// * `default_levels`  -   If the compressor has never been run on this room before
///                         Then we need to provide the compressor with some information
///                         on what sort of compression structure we want. The default that
///                         the library suggests is `vec!((100,0,None),(50,0,None),(25,0,None))`
pub fn run_compressor_on_room_chunk(
    db_url: &str,
    room_id: &str,
    chunk_size: i64,
    default_levels: &[LevelState],
) -> Result<Option<ChunkStats>> {
    // connect to the database
    let mut client = match connect_to_database(db_url) {
        Ok(c) => c,
        Err(e) => bail!("Error while connecting to {}: {}", db_url, e),
    };

    // Access the database to find out where the compressor last got up to
    let retrieved_state = match read_room_compressor_state(&mut client, room_id) {
        Ok(s) => s,
        Err(e) => bail!(
            "Error while reading compressor state for room {}: {}",
            room_id,
            e
        ),
    };

    // If the database didn't contain any information, then use the default state
    let (start, level_info) = match retrieved_state {
        Some((s, l)) => (Some(s), l),
        None => (None, default_levels.to_vec()),
    };

    // run the compressor on this chunk
    let option_chunk_stats = continue_run(start, chunk_size, db_url, room_id, &level_info);

    if option_chunk_stats.is_none() {
        debug!("No work to do on this room...");
        return Ok(None);
    }

    // Ok to unwrap because have checked that it's not none
    let chunk_stats = option_chunk_stats.unwrap();

    debug!("{:?}", chunk_stats);

    // Check to see whether the compressor sent its changes to the database
    if !chunk_stats.commited {
        warn!(
            "The compressor tried to increase the number of rows in {} between {:?} and {}. Skipping...",
            room_id, start, chunk_stats.last_compressed_group,
        );

        // Skip over the failed chunk and set the level info to the default (empty) state
        let write_result = write_room_compressor_state(
            &mut client,
            room_id,
            default_levels,
            chunk_stats.last_compressed_group,
        );

        if let Err(e) = write_result {
            bail!(
                "Error when skipping chunk in room {} between {:?} and {}: {}",
                room_id,
                start,
                chunk_stats.last_compressed_group,
                e,
            )
        }

        return Ok(Some(chunk_stats));
    }

    // Save where we got up to after this successful commit
    let write_result = write_room_compressor_state(
        &mut client,
        room_id,
        &chunk_stats.new_level_info,
        chunk_stats.last_compressed_group,
    );

    if let Err(e) = write_result {
        bail!(
            "Error when saving state after compressing chunk in room {} between {:?} and {}: {}",
            room_id,
            start,
            chunk_stats.last_compressed_group,
            e,
        )
    }

    Ok(Some(chunk_stats))
}

/// Compresses a chunk of the 1st room in the rooms_to_compress argument given
///
/// If the 1st room has no more groups to compress then it
/// removes that room from the rooms_to_compress vector
///
/// # Arguments
///
/// * `db_url`          -   The URL of the postgres database that synapse is using.
///                         e.g. "postgresql://user:password@domain.com/synapse"
///
/// * `chunk_size`      -   The number of state_groups to work on. All of the entries
///                         from state_groups_state are requested from the database
///                         for state groups that are worked on. Therefore small
///                         chunk sizes may be needed on machines with low memory.
///                         (Note: if the compressor fails to find space savings on the
///                         chunk as a whole (which may well happen in rooms with lots
///                         of backfill in) then the entire chunk is skipped.)
///
/// * `default_levels`  -   If the compressor has never been run on this room before
///                         Then we need to provide the compressor with some information
///                         on what sort of compression structure we want. The default that
///                         the library suggests is `vec!((100,0,None),(50,0,None),(25,0,None))`
///
/// * `rooms_to_compress` - A vector of (room_id, number of uncompressed rows) tuples. This is a
///                         list of all the rooms that need compressing
fn compress_chunk_of_largest_room(
    db_url: &str,
    chunk_size: i64,
    default_levels: &[LevelState],
    rooms_to_compress: &mut Vec<(String, i64)>,
) -> Result<Option<ChunkStats>> {
    if rooms_to_compress.is_empty() {
        bail!("Called compress_chunk_of_largest_room with empty rooms_to_compress argument");
    }

    // can call unwrap safely as have checked that rooms_to_compress is not empty
    let (room_id, _) = rooms_to_compress.get(0).unwrap().clone();

    debug!(
        "Running compressor on room {} with chunk size {}",
        room_id, chunk_size
    );

    let work_done = run_compressor_on_room_chunk(db_url, &room_id, chunk_size, default_levels)?;

    if work_done.is_none() {
        rooms_to_compress.remove(0);
    }

    Ok(work_done)
}

/// Runs the compressor (in chunks) on the rooms with the most uncompressed state
///
/// # Arguments
///
/// * `db_url`          -   The URL of the postgres database that synapse is using.
///                         e.g. "postgresql://user:password@domain.com/synapse"
///
/// * `chunk_size`      -   The number of state_groups to work on. All of the entries
///                         from state_groups_state are requested from the database
///                         for state groups that are worked on. Therefore small
///                         chunk sizes may be needed on machines with low memory.
///                         (Note: if the compressor fails to find space savings on the
///                         chunk as a whole (which may well happen in rooms with lots
///                         of backfill in) then the entire chunk is skipped.)
///
/// * `default_levels`  -   If the compressor has never been run on this room before
///                         Then we need to provide the compressor with some information
///                         on what sort of compression structure we want. The default that
///                         the library suggests is `vec!((100,0,None),(50,0,None),(25,0,None))`
///
/// * `number`          -   The number of rooms to compress. The larger this number is, the more
///                         work that can be done before having to scan through the database again
///                         to cound the uncompressed rows
pub fn compress_largest_rooms(
    db_url: &str,
    chunk_size: i64,
    default_levels: &[LevelState],
    number: i64,
) -> Result<()> {
    // connect to the database
    let mut client = match connect_to_database(db_url) {
        Ok(c) => c,
        Err(e) => bail!("Error while connecting to {}: {}", db_url, e),
    };

    if let Err(e) = create_tables_if_needed(&mut client) {
        bail!(
            "Error while attempting to create state compressor tables: {}",
            e
        );
    }

    let rooms_to_compress = match get_rooms_with_most_rows_to_compress(&mut client, number) {
        Ok(r) => r,
        Err(e) => bail!(
            "Error while trying to work out what room to compress next: {}",
            e
        ),
    };

    if rooms_to_compress.is_none() {
        return Ok(());
    }

    // can unwrap safely as have checked is not none
    let mut rooms_to_compress = rooms_to_compress.unwrap();

    let mut skipped_chunks = 0;
    let mut rows_saved = 0;
    let mut chunks_processed = 0;
    let mut first_pass = true;

    while !rooms_to_compress.is_empty() {
        if first_pass {
            // can unwrap as have checked rooms_to_compress is not empty
            info!("Compressing: {}", rooms_to_compress.get(0).unwrap().0);
            first_pass = false;
        }

        let work_done = compress_chunk_of_largest_room(
            db_url,
            chunk_size,
            default_levels,
            &mut rooms_to_compress,
        )?;

        if let Some(ref chunk_stats) = work_done {
            if chunk_stats.commited {
                rows_saved += chunk_stats.original_num_rows - chunk_stats.new_num_rows;
            } else {
                skipped_chunks += 1;
            }
            chunks_processed += 1;
        } else {
            info!(
                "Finished compressing room. Saved {} rows. Skipped {}/{} chunks",
                rows_saved, skipped_chunks, chunks_processed
            );
            first_pass = true;
        }
    }
    Ok(())
}
