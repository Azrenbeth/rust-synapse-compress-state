// This module contains functions that carry out diffferent types
// of compression on the database.

use crate::{
    state_saving::{
        connect_to_database, get_rooms_with_most_rows_to_compress, read_room_compressor_state,
        write_room_compressor_state,
    },
    LevelState,
};
use synapse_compress_state::continue_run;

/// Runs the compressor on a chunk of the room
///
/// Returns `true` if the compressor has progressed
/// and `false` if it had already got to the end of the room
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
) -> bool {
    // connect to the database
    let mut client = connect_to_database(db_url)
        .unwrap_or_else(|e| panic!("Error while connecting to {}: {}", db_url, e));

    // Access the database to find out where the compressor last got up to
    let retrieved_state = read_room_compressor_state(&mut client, room_id).unwrap_or_else(|e| {
        panic!(
            "Unable to read compressor state for room {}: {}",
            room_id, e
        )
    });

    // If the database didn't contain any information, then use the default state
    let (start, level_info) = match retrieved_state {
        Some((s, l)) => (Some(s), l),
        None => (None, default_levels.to_vec()),
    };

    // run the compressor on this chunk
    let option_chunk_stats = continue_run(start, chunk_size, db_url, room_id, &level_info);

    if option_chunk_stats.is_none() {
        return false;
    }

    let chunk_stats = option_chunk_stats.unwrap();

    println!("{:?}", chunk_stats);

    // Check to see whether the compressor sent its changes to the database
    if !chunk_stats.commited {
        println!(
            "The compressor tried to increase the number of rows in {} between {:?} and {}. Skipping...",
            room_id, start, chunk_stats.last_compressed_group,
        );

        // Skip over the failed chunk and set the level info to the default (empty) state
        write_room_compressor_state(
            &mut client,
            room_id,
            &default_levels,
            chunk_stats.last_compressed_group,
        )
        .unwrap_or_else(|e| {
            panic!(
                "Error when skipping chunk in room {} between {:?} and {}: {}",
                room_id, start, chunk_stats.last_compressed_group, e,
            )
        });

        return true;
    }

    // Save where we got up to after this successful commit
    write_room_compressor_state(
        &mut client,
        room_id,
        &chunk_stats.new_level_info,
        chunk_stats.last_compressed_group,
    )
    .unwrap_or_else(|e| {
        panic!(
            "Error when saving state after compressing chunk in room {} between {:?} and {}: {}",
            room_id, start, chunk_stats.last_compressed_group, e,
        )
    });

    true
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
) {
    if rooms_to_compress.is_empty() {
        panic!("Called compress_chunk_of_largest_room with empty rooms_to_compress argument");
    }

    let (room_id, _) = rooms_to_compress.get(0).unwrap().clone();

    let did_work = run_compressor_on_room_chunk(db_url, &room_id, chunk_size, default_levels);

    if !did_work {
        rooms_to_compress.remove(0);
    }
}

/// Runs the compressor on a chunk of the room
///
/// Returns `true` if the compressor has progressed
/// and `false` if it had already got to the end of the room
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
) {
    // connect to the database
    let mut client = connect_to_database(db_url)
        .unwrap_or_else(|e| panic!("Error while connecting to {}: {}", db_url, e));

    let rooms_to_compress = get_rooms_with_most_rows_to_compress(&mut client, number)
        .unwrap_or_else(|e| {
            panic!(
                "Error while trying to work out what room to compress next: {}",
                e
            )
        });

    if rooms_to_compress.is_none() {
        return;
    }

    let mut rooms_to_compress = rooms_to_compress.unwrap();

    while !rooms_to_compress.is_empty() {
        compress_chunk_of_largest_room(db_url, chunk_size, default_levels, &mut rooms_to_compress);
    }
}
