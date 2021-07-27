// Copyright 2018 New Vector Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use indicatif::{ProgressBar, ProgressStyle};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::{fallible_iterator::FallibleIterator, types::ToSql, Client};
use postgres_openssl::MakeTlsConnector;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::{borrow::Cow, collections::BTreeMap, fmt};

use super::StateGroupEntry;

/// Fetch the entries in state_groups_state (and their prev groups) for a
/// specific room.
///
/// Returns with the state_group map and the id of the last group that was used
/// Or None if there are no state groups within the range given
///
/// # Arguments
///
/// * `room_id`             -   The ID of the room in the database
/// * `db_url`              -   The URL of a Postgres database. This should be of the
///                             form: "postgresql://user:pass@domain:port/database"
/// * `min_state_group`     -   If specified, then only fetch the entries for state
///                             groups greater than (but not equal) to this number. It
///                             also requires groups_to_compress to be specified
/// * 'groups_to_compress'  -   The number of groups to get from the database before stopping

pub fn get_data_from_db(
    db_url: &str,
    room_id: &str,
    min_state_group: Option<i64>,
    groups_to_compress: Option<i64>,
) -> Option<(BTreeMap<i64, StateGroupEntry>, i64)> {
    // connect to the database
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let mut client = Client::connect(db_url, connector).unwrap();

    let state_group_map: BTreeMap<i64, StateGroupEntry> = BTreeMap::new();

    // Search for the group id of the groups_to_compress'th group after min_state_group
    // If this is saved, then the compressor can continue by having min_state_group being
    // set to this maximum. Max_group_found is NONE if there are no groups to compress
    // in this range.
    let max_group_found = find_max_group(&mut client, room_id, min_state_group, groups_to_compress)?;

    Some(load_map_from_db(
        &mut client,
        room_id,
        min_state_group,
        max_group_found,
        state_group_map,
    ))
}

/// Fetch the entries in state_groups_state (and their prev groups) for a
/// specific room. This method should only be called if resuming the compressor from
/// where it last finished - and as such also loads in the state groups from the heads
/// of each of the levels (as they were at the end of the last run of the compressor)
///
/// Returns with the state_group map and the id of the last group that was used
/// Or None if there are no state groups within the range given
///
/// # Arguments
///
/// * `room_id`             -   The ID of the room in the database
/// * `db_url`              -   The URL of a Postgres database. This should be of the
///                             form: "postgresql://user:pass@domain:port/database"
/// * `min_state_group`     -   If specified, then only fetch the entries for state
///                             groups greater than (but not equal) to this number. It
///                             also requires groups_to_compress to be specified
/// * 'groups_to_compress'  -   The number of groups to get from the database before stopping
/// * 'level_info'          -   The maximum size, current length and current head for each
///                             level (as it was when the compressor last finished for this
///                             room)

pub fn reload_data_from_db(
    db_url: &str,
    room_id: &str,
    min_state_group: Option<i64>,
    groups_to_compress: Option<i64>,
    level_info: &[(usize, usize, Option<i64>)],
) -> Option<(BTreeMap<i64, StateGroupEntry>, i64)> {
    // connect to the database
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let mut client = Client::connect(db_url, connector).unwrap();

    // Search for the group id of the groups_to_compress'th group after min_state_group
    // If this is saved, then the compressor can continue by having min_state_group being
    // set to this maximum. Max_group_found is NONE if there are no groups to compress
    // in this range.
    let max_group_found = find_max_group(&mut client, room_id, min_state_group, groups_to_compress)?;

    // load just the state_groups at the head of each level
    // this doesn't load their predecessors as that will be done at the end of
    // load_map_from_db()
    let state_group_map: BTreeMap<i64, StateGroupEntry> = load_level_heads(&mut client, level_info);

    Some(load_map_from_db(
        &mut client,
        room_id,
        min_state_group,
        max_group_found,
        state_group_map,
    ))
}

/// Finds the state_groups that are at the head of each compressor level
/// NOTE this does not also retrieve their predecessors
///
/// # Arguments
///
/// * `client'  -   A Postgres client to make requests with
/// * `levels'  -   The levels who's heads are being requested
fn load_level_heads(
    client: &mut Client,
    level_info: &[(usize, usize, Option<i64>)],
) -> BTreeMap<i64, StateGroupEntry> {
    // obtain all of the heads that aren't None from level_info
    let level_heads: Vec<i64> = level_info
        .iter()
        .map(|(_, _, head)| {
            if let Some(c) = *head {
                return c;
            }
            -1
        })
        .filter(|c| *c > 0)
        .collect();

    // Query to get id, predecessor and deltas for each state group
    let sql = r#"
        SELECT m.id, prev_state_group, type, state_key, s.event_id
        FROM state_groups AS m
        LEFT JOIN state_groups_state AS s ON (m.id = s.state_group)
        LEFT JOIN state_group_edges AS e ON (m.id = e.state_group)
        WHERE m.id = ANY($1)
    "#;

    // Actually do the query
    let mut rows = client.query_raw(sql, &[&level_heads]).unwrap();

    // Copy the data from the database into a map
    let mut state_group_map: BTreeMap<i64, StateGroupEntry> = BTreeMap::new();

    while let Some(row) = rows.next().unwrap() {
        // The row in the map to copy the data to
        // NOTE: default StateGroupEntry has in_range as false
        // This is what we want since as a level head, it has already been compressed by the
        // previous run!
        let entry = state_group_map.entry(row.get(0)).or_default();

        // Save the predecessor (this may already be there)
        entry.prev_state_group = row.get(1);

        // Copy the single delta from the predecessor stored in this row
        if let Some(etype) = row.get::<_, Option<String>>(2) {
            entry.state_map.insert(
                &etype,
                &row.get::<_, String>(3),
                row.get::<_, String>(4).into(),
            );
        }
    }
    state_group_map
}

/// Fetch the entries in state_groups_state (and their prev groups) for a
/// specific room within a certain range. These are appended onto the provided
/// map.
///
/// - Fetches the first [group] rows with group id after [min]
/// - Recursively searches for missing predecessors and adds those
///
/// Returns with the state_group map and the id of the last group that was used
///
/// # Arguments
///
/// * `client`              -   A Postgres client to make requests with
/// * `room_id`             -   The ID of the room in the database
/// * `min_state_group`     -   If specified, then only fetch the entries for state
///                             groups greater than (but not equal) to this number. It
///                             also requires groups_to_compress to be specified
/// * 'max_group_found'     -   The last group to get from the database before stopping
/// * 'state_group_map'     -   The map to populate with the entries from the database

fn load_map_from_db(
    client: &mut Client,
    room_id: &str,
    min_state_group: Option<i64>,
    max_group_found: i64,
    mut state_group_map: BTreeMap<i64, StateGroupEntry>,
) -> (BTreeMap<i64, StateGroupEntry>, i64) {
    
    state_group_map.append(&mut get_initial_data_from_db(
        client,
        room_id,
        min_state_group,
        max_group_found,
    ));

    println!("Got initial state from database. Checking for any missing state groups...");

    // Due to reasons some of the state groups appear in the edges table, but
    // not in the state_groups_state table.
    //
    // Also it is likely that the predecessor of a node will not be within the
    // chunk that was specified by min_state_group and groups_to_compress.
    // This means they don't get included in our DB queries, so we have to fetch
    // any missing groups explicitly.
    //
    // Since the returned groups may themselves reference groups we don't have,
    // we need to do this recursively until we don't find any more missing.
    loop {
        let mut missing_sgs: Vec<_> = state_group_map
            .iter()
            .filter_map(|(_sg, entry)| {
                if let Some(prev_sg) = entry.prev_state_group {
                    if state_group_map.contains_key(&prev_sg) {
                        None
                    } else {
                        Some(prev_sg)
                    }
                } else {
                    None
                }
            })
            .collect();

        if missing_sgs.is_empty() {
            // println!("No missing state groups");
            break;
        }

        missing_sgs.sort_unstable();
        missing_sgs.dedup();

        // println!("Missing {} state groups", missing_sgs.len());

        // find state groups not picked up already and add them to the map
        let map = get_missing_from_db(client, &missing_sgs, min_state_group, max_group_found);
        for (k, v) in map.into_iter() {
            state_group_map.entry(k).or_insert(v);
        }
    }

    (state_group_map, max_group_found)
}

/// Returns the group ID of the last group to be compressed
///
/// This can be saved so that future runs of the compressor only
/// continue from after this point
///
/// # Arguments
///
/// * `client`              -   A Postgres client to make requests with
/// * `room_id`             -   The ID of the room in the database
/// * `min_state_group`     -   The lower limit (non inclusive) of group id's to compress
/// * 'groups_to_compress'  -   How many groups to compress

fn find_max_group(
    client: &mut Client,
    room_id: &str,
    min_state_group: Option<i64>,
    groups_to_compress: Option<i64>,
) -> Option<i64> {
    let sql = r#"
        SELECT m.id
        FROM state_groups AS m
        WHERE m.room_id = $1
    "#;

    // Adds additional constraint if a groups_to_compress or min_state_group have been specified
    // Then sends query to the datatbase
    let rows = if let (Some(min), Some(count)) = (min_state_group, groups_to_compress) {
        let params: Vec<&dyn ToSql> = vec![&room_id, &min, &count];
        client.query_raw(format!(r"{} AND m.id > $2 LIMIT $3", sql).as_str(), params)
    } else if let Some(count) = groups_to_compress {
        let params: Vec<&dyn ToSql> = vec![&room_id, &count];
        client.query_raw(format!(r"{} LIMIT $2", sql).as_str(), params)
    } else {
        client.query_raw(sql, &[room_id])

    }
    .unwrap();

    let final_row = rows.last().unwrap().unwrap();
    Some(final_row.get::<_,i64>(0))
}

/// Fetch the entries in state_groups_state and immediate predecessors for
/// a specific room.
///
/// - Fetches first [groups_to_compress] rows with group id higher than min
/// - Stores the group id, predecessor id and deltas into a map
/// - returns map and maximum row that was considered
///
/// # Arguments
///
/// * `client`          -   A Postgres client to make requests with
/// * `room_id`         -   The ID of the room in the database
/// * `min_state_group` -   If specified, then only fetch the entries for state
///                         groups greater than (but not equal) to this number. It
///                         also requires groups_to_compress to be specified
/// * 'max_group_found' -   The upper limit on state_groups ids to get from the database

fn get_initial_data_from_db(
    client: &mut Client,
    room_id: &str,
    min_state_group: Option<i64>,
    max_group_found: i64,
) -> BTreeMap<i64, StateGroupEntry> {
    // Query to get id, predecessor and deltas for each state group
    let sql = r#"
        SELECT m.id, prev_state_group, type, state_key, s.event_id
        FROM state_groups AS m
        LEFT JOIN state_groups_state AS s ON (m.id = s.state_group)
        LEFT JOIN state_group_edges AS e ON (m.id = e.state_group)
        WHERE m.room_id = $1
    "#;

    // Adds additional constraint if minimum state_group has been specified.
    let mut rows = if let Some(min) = min_state_group {
        let params: Vec<&dyn ToSql> = vec![&room_id, &min, &max_group_found];
        client.query_raw(
            format!(r"{} AND m.id > $2 AND m.id <= $3", sql).as_str(),
            params,
        )
    } else {
        let params: Vec<&dyn ToSql> = vec![&room_id, &max_group_found];
        client.query_raw(
            format!(r"{} AND m.id <= $2", sql).as_str(),
            params,
        )
    }
    .unwrap();

    // Copy the data from the database into a map
    let mut state_group_map: BTreeMap<i64, StateGroupEntry> = BTreeMap::new();

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner().template("{spinner} [{elapsed}] {pos} rows retrieved"),
    );
    pb.enable_steady_tick(100);

    while let Some(row) = rows.next().unwrap() {
        // The row in the map to copy the data to
        let entry = state_group_map.entry(row.get(0)).or_default();

        // Save the predecessor and mark for compression (this may already be there)
        // TODO: slightly fewer redundant rewrites
        entry.prev_state_group = row.get(1);
        entry.in_range = true;

        // Copy the single delta from the predecessor stored in this row
        if let Some(etype) = row.get::<_, Option<String>>(2) {
            entry.state_map.insert(
                &etype,
                &row.get::<_, String>(3),
                row.get::<_, String>(4).into(),
            );
        }

        pb.inc(1);
    }

    pb.set_length(pb.position());
    pb.finish();

    state_group_map
}

/// Finds the predecessors of missing state groups
///
/// N.B. this does NOT find their deltas
///
/// # Arguments
///
/// * `client`          -   A Postgres client to make requests with
/// * `missing_sgs`     -   An array of missing state_group ids
/// * 'min_state_group' -   Minimum state_group id to mark as in range
/// * 'max_group_found' -   Maximum state_group id to mark as in range

fn get_missing_from_db(
    client: &mut Client,
    missing_sgs: &[i64],
    min_state_group: Option<i64>,
    max_group_found: i64,
) -> BTreeMap<i64, StateGroupEntry> {
    // "Due to reasons" it is possible that some states only appear in edges table and not in state_groups table
    // so since we know the IDs we're looking for as they are the missing predecessors, we can find them by
    // left joining onto the edges table (instead of the state_group table!)
    let sql = r#"
        SELECT target.prev_state_group, source.prev_state_group, state.type, state.state_key, state.event_id
        FROM state_group_edges AS target
        LEFT JOIN state_group_edges AS source ON (target.prev_state_group = source.state_group)
        LEFT JOIN state_groups_state AS state ON (target.prev_state_group = state.state_group)
        WHERE target.prev_state_group = ANY($1)
    "#;

    let mut rows = client.query_raw(sql, &[missing_sgs]).unwrap();

    let mut state_group_map: BTreeMap<i64, StateGroupEntry> = BTreeMap::new();

    while let Some(row) = rows.next().unwrap() {
        let id = row.get(0);
        // The row in the map to copy the data to
        let entry = state_group_map.entry(id).or_default();

        // Save the predecessor and mark for compression (this may already be there)
        // Also may well not exist!
        entry.prev_state_group = row.get(1);
        if let Some(min) = min_state_group {
            if min < id && id > max_group_found {
                entry.in_range = true
            }
        }

        // Copy the single delta from the predecessor stored in this row
        if let Some(etype) = row.get::<_, Option<String>>(2) {
            entry.state_map.insert(
                &etype,
                &row.get::<_, String>(3),
                row.get::<_, String>(4).into(),
            );
        }
    }

    state_group_map
}

// TODO: find a library that has an existing safe postgres escape function
/// Helper function that escapes the wrapped text when writing SQL
pub struct PGEscape<'a>(pub &'a str);

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

#[test]
fn test_pg_escape() {
    let s = format!("{}", PGEscape("test"));
    assert_eq!(s, "$$test$$");

    let dodgy_string = "test$$ing";

    let s = format!("{}", PGEscape(dodgy_string));

    // prefix and suffixes should match
    let start_pos = s.find(dodgy_string).expect("expected to find dodgy string");
    let end_pos = start_pos + dodgy_string.len();
    assert_eq!(s[..start_pos], s[end_pos..]);

    // .. and they should start and end with '$'
    assert_eq!(&s[0..1], "$");
    assert_eq!(&s[start_pos - 1..start_pos], "$");
}

///
/// Note that currently ignores config.transactions and wraps every state
/// group in it's own transaction (i.e. as if config.transactions was true)
///
/// # Arguments
///
/// * `config` -    A Config struct that contains information
///                 about the run (e.g. room_id and database url)
/// * `old_map` -   The state group data originally in the database
/// * `new_map` -   The state group data generated by the compressor to
///                 replace replace the old contents
pub fn send_changes_to_db(
    db_url: &str,
    room_id: &str,
    old_map: &BTreeMap<i64, StateGroupEntry>,
    new_map: &BTreeMap<i64, StateGroupEntry>,
) {
    // connect to the database
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let mut client = Client::connect(db_url, connector).unwrap();

    println!("Writing changes...");

    // setup the progress bar
    let pb = ProgressBar::new(old_map.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar().template("[{elapsed_precise}] {bar} {pos}/{len} {msg}"),
    );
    pb.set_message("state groups");
    pb.enable_steady_tick(100);

    // Go through all groups in the old_map (what is currently in the database)
    for (sg, old_entry) in old_map {
        let new_entry = &new_map[sg];

        // Check if the new map has a different entry for this state group
        // N.B. also checks if in_range fields agree
        if old_entry != new_entry {
            // the sql commands that will carry out these changes
            let mut sql = "".to_string();

            // remove the current edge
            sql.push_str(&format!(
                "DELETE FROM state_group_edges WHERE state_group = {};\n",
                sg
            ));

            // if the new entry has a predecessor then put that into state_group_edges
            if let Some(prev_sg) = new_entry.prev_state_group {
                sql.push_str(&format!("INSERT INTO state_group_edges (state_group, prev_state_group) VALUES ({}, {});\n", sg, prev_sg));
            }

            // remove the current deltas for this state group
            sql.push_str(&format!(
                "DELETE FROM state_groups_state WHERE state_group = {};\n",
                sg
            ));

            if !new_entry.state_map.is_empty() {
                // place all the deltas for the state group in the new map into state_groups_state
                sql.push_str("INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES\n");

                let mut first = true;
                for ((t, s), e) in new_entry.state_map.iter() {
                    // Add a comma at the start if not the first row to be inserted
                    if first {
                        sql.push_str("     ");
                        first = false;
                    } else {
                        sql.push_str("    ,");
                    }

                    // write the row to be insterted of the form:
                    // (state_group, room_id, type, state_key, event_id)
                    sql.push_str(&format!(
                        "({}, {}, {}, {}, {})",
                        sg,
                        PGEscape(room_id),
                        PGEscape(t),
                        PGEscape(s),
                        PGEscape(e)
                    ));
                }
                sql.push_str(";\n");
            }

            // commit this change to the database
            // N.B. this is a synchronous library so will wait until finished before continueing...
            // if want to speed up compressor then this might be a good place to start!
            let mut single_group_transaction = client.transaction().unwrap();
            single_group_transaction.batch_execute(&sql).unwrap();
            single_group_transaction.commit().unwrap();
        }

        pb.inc(1);
    }

    pb.finish();
}
