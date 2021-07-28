use core::fmt;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::borrow::Cow;

use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::{fallible_iterator::FallibleIterator, Client, Error};
use postgres_openssl::MakeTlsConnector;

use crate::LevelState;

// Connects to the database and returns the client to use for the rest of the program
pub fn connect_to_database(db_url: &str) -> Result<Client, Error> {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    Client::connect(db_url, connector)
}

// Creates the state_compressor_state and state_compressor progress tables
// in the database if they don't already exist
pub fn create_tables_if_needed(client: &mut Client) -> Result<u64, Error> {
    let create_state_table = r#"
        CREATE TABLE IF NOT EXISTS state_compressor_state (
            room_id TEXT NOT NULL,
            level_num INT NOT NULL,
            max_size INT NOT NULL,
            current_length INT NOT NULL,
            current_head BIGINT
        )"#;

    client.execute(create_state_table, &[])?;

    let create_progress_table = r#"
        CREATE TABLE IF NOT EXISTS state_compressor_progress (
            room_id TEXT NOT NULL,
            last_compressed BIGINT NOT NULL
        )"#;

    client.execute(create_progress_table, &[])
}

// Retrieve the level info so we can restart the compressor
// on a given room
pub fn read_room_compressor_state(
    client: &mut Client,
    room_id: &str,
) -> Result<Option<(i64, Vec<LevelState>)>, Error> {
    // Query to retrieve all levels from state_compressor_state
    // Ordered by ascending level_number
    let sql = r#"
        SELECT level_num, max_size, current_length, current_head, last_compressed
        FROM state_compressor_state as s
        LEFT JOIN state_compressor_progress as p ON s.room_id = p.room_id
        WHERE s.room_id = $1
        ORDER BY level_num ASC
    "#;

    // send the query to the database
    let mut levels = client.query_raw(sql, &[room_id])?;

    // Needed to ensure that the rows are for unique consecutive levels
    // starting from 1 (i.e of form [1,2,3] not [0,1,2] or [1,1,2,2,3])
    let mut prev_seen = 0;

    // The vector to store the level info from the database in
    let mut level_info: Vec<LevelState> = Vec::new();
    let mut last_compressed = 0;

    // Loop through all the rows retrieved by that query
    while let Some(l) = levels.next()? {
        // read out the fields into variables
        let level_num: usize = l.get::<_, i32>(0) as usize;
        let max_size: usize = l.get::<_, i32>(1) as usize;
        let current_length: usize = l.get::<_, i32>(2) as usize;
        let current_head: Option<i64> = l.get::<_, Option<i64>>(3);
        last_compressed = l.get::<_, i64>(4); // possibly rewrite same value

        // Check that there aren't multiple entries for the same level number
        // in the database.
        if prev_seen == level_num {
            panic!(
                "The level {} occurs twice in state_compressor_state for room {}",
                level_num, room_id,
            );
        }

        // Check that there is no missing level in the database
        // e.g. if the previous row retrieved was for level 1 and this
        // row is for level 3 then since the SQL query orders the results
        // in ascenting level numbers, there was no level 2 found!
        if prev_seen != level_num - 1 {
            panic!("Levels between {} and {} are missing", prev_seen, level_num,);
        }

        // if the level is not empty, then it must have a head!
        if current_head.is_none() && current_length != 0 {
            panic!(
                "Level {} has no head but current length is {} in room {}",
                level_num, current_length, room_id,
            )
        }

        // If the level has more groups in than the maximum then something is wrong!
        if current_length > max_size {
            panic!(
                "Level {} has length {} but max size {} in room {}",
                level_num, current_length, max_size, room_id,
            );
        }

        // Add this level to the level_info vector
        level_info.push((max_size, current_length, current_head));
        // Mark the previous level_number seen as the current one
        prev_seen = level_num;
    }

    // If we didn't retrieve anything from the database then there is no saved state
    // in the database!
    if level_info.is_empty() {
        return Ok(None);
    }

    // Return the compressor state we retrieved
    Ok(Some((last_compressed, level_info)))
}

// Save the level info so it can be loaded by the next run of the compressor
// on the same room
pub fn write_room_compressor_state(
    client: &mut Client,
    room_id: &str,
    level_info: &[LevelState],
    last_compressed: i64,
) -> Result<(), Error> {
    // The query we are going to build up
    let mut sql = "".to_string();

    // Go through every level that the compressor is using
    for (level_num, level) in level_info.iter().enumerate() {
        // the 1st level is level 1 not level 0, but enumerate starts at 0
        // so need to add 1 to get correct number
        let level_num = level_num + 1;

        // delete the old information for this level from the database
        sql.push_str(&format!(
            "DELETE FROM state_compressor_state WHERE room_id = {} AND level_num = {};\n",
            PGEscape(room_id),
            level_num,
        ));

        // bring the level info out of the tuple
        let (max_size, current_len, current_head) = level;

        // Current_head is either a value or NULL
        // need to convert from Option so that this can be placed into a string
        let current_head = match current_head {
            Some(s) => s.to_string(),
            None => "NULL".to_string(),
        };

        // Add the new informaton for this level into the database
        sql.push_str(&format!(
            concat!(
                "INSERT INTO state_compressor_state (room_id, level_num, max_size,",
                " current_length, current_head) VALUES ({}, {}, {}, {}, {});"
            ),
            PGEscape(room_id),
            level_num,
            max_size,
            current_len,
            current_head,
        ));
    }

    // delete the old information for this level from the database
    sql.push_str(&format!(
        "DELETE FROM state_compressor_progress WHERE room_id = {};\n",
        PGEscape(room_id),
    ));

    // Add the new informaton for this level into the database
    sql.push_str(&format!(
        "INSERT INTO state_compressor_progress (room_id, last_compressed) VALUES ({},{});",
        PGEscape(room_id),
        last_compressed,
    ));

    // Wrap all the changes to the state for this room in a transaction
    // This prevents accidentally having malformed compressor start info
    let mut write_transaction = client.transaction()?;
    write_transaction.batch_execute(&sql)?;
    write_transaction.commit()?;

    Ok(())
}

// TODO: find a library that has an existing safe postgres escape function
/// Helper function that escapes the wrapped text when writing SQL
struct PGEscape<'a>(pub &'a str);

impl<'a> fmt::Display for PGEscape<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut delim = Cow::from("$$");
        while self.0.contains(&delim as &str) {
            let s: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from)
                .collect();

            delim = format!("${}$", s).into();
        }

        write!(f, "{}{}{}", delim, self.0, delim)
    }
}

pub fn get_rooms_with_most_rows_to_compress(
    client: &mut Client,
    number: i64,
) -> Result<Option<Vec<(String, i64)>>, Error> {
    let get_biggest_rooms = r#"
        SELECT s.room_id, count(*) AS num_rows
        FROM state_groups_state AS s
        LEFT JOIN state_compressor_progress AS p 
            ON s.room_id = p.room_id
        WHERE s.state_group > p.last_compressed 
            OR p.last_compressed IS NULL
        GROUP BY s.room_id
        ORDER BY num_rows
        LIMIT $1
    "#;

    let rooms = client.query(get_biggest_rooms, &[&number])?;

    if rooms.is_empty() {
        return Ok(None);
    }

    let rows_to_compress = rooms
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
        .collect();

    Ok(Some(rows_to_compress))
}