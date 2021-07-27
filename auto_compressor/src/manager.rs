use crate::{
    state_saving::{connect_to_database, read_room_compressor_state, write_room_compressor_state},
    LevelState,
};
use synapse_compress_state::continue_run;

/// Runs the compressor on a chunk of the room
///
/// Returns true if the compressor has progressed
/// And false if it had already got to the end of the room
pub fn run_compressor_on_room_chunk(
    db_url: &str,
    room_id: &str,
    chunk_size: i64,
    default_levels: &[LevelState],
) -> bool {
    // connect to the database
    let mut client = connect_to_database(db_url)
        .unwrap_or_else(|_| panic!("Error while connecting to {}", db_url));

    // Access the database to find out where the compressor last got up to
    let retrieved_state = read_room_compressor_state(&mut client, room_id)
        .unwrap_or_else(|_| panic!("Unable to read compressor state for room {}", room_id));

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
        .unwrap_or_else(|_| {
            panic!(
                "Error when skipping chunk in room {} between {:?} and {}",
                room_id, start, chunk_stats.last_compressed_group,
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
    .unwrap_or_else(|_| {
        panic!(
            "Error when saving state after compressing chunk in room {} between {:?} and {}",
            room_id, start, chunk_stats.last_compressed_group,
        )
    });

    true
}

/// Compresses a chunk of the 1st room in rooms_to_compress
///
/// If the 1st room has no more groups to compress then it
/// removes that room from the rooms_to_compress vector
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
