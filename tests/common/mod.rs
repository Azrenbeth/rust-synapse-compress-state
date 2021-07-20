use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::Client;
use postgres_openssl::MakeTlsConnector;
use state_map::StateMap;
use string_cache::DefaultAtom as Atom;
use std::collections::BTreeMap;

use synapse_compress_state::{PGEscape, StateGroupEntry};

static DB_URL: &str = "postgresql://synapse_user:synapse_pass@localhost/synapse";

pub fn add_contents_to_database(room_id: &str, state_group_map: &BTreeMap<i64, StateGroupEntry>) {
    // connect to the database
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let mut client = Client::connect(DB_URL, connector).unwrap();

    // build up the query
    let mut sql = "".to_string();

    for (sg, entry) in state_group_map {
        // create the entry for state_groups
        sql.push_str(&format!(
            "INSERT INTO state_groups (id, room_id, event_id) VALUES ({},{},{});\n",
            sg,
            PGEscape(room_id),
            PGEscape("left_blank")
        ));

        // create the entry in state_group_edges IF exists
        if let Some(prev_sg) = entry.prev_state_group {
            sql.push_str(&format!(
                "INSERT INTO state_group_edges (state_group, prev_state_group) VALUES ({}, {});\n",
                sg, prev_sg
            ));
        }

        // write entry for each row in delta
        if !entry.state_map.is_empty() {
            sql.push_str("INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES");

            let mut first = true;
            for ((t, s), e) in entry.state_map.iter() {
                if first {
                    sql.push_str("     ");
                    first = false;
                } else {
                    sql.push_str("    ,");
                }
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
    }

    client.batch_execute(&sql).unwrap();
}

pub fn empty_database() {
    // connect to the database
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let mut client = Client::connect(DB_URL, connector).unwrap();

    // delete all the contents from all three tables
    let mut sql = "".to_string();
    sql.push_str("DELETE FROM state_groups;\n");
    sql.push_str("DELETE FROM state_group_edges;\n");
    sql.push_str("DELETE FROM state_groups_state;\n");

    client.batch_execute(&sql).unwrap();
}

pub fn database_matches_map(room_id: &str, state_group_map: &BTreeMap<i64, StateGroupEntry>) -> bool {
    false
}

/// Gets the full state for a given group from the map (of deltas)
fn collapse_state_with_map(map: &BTreeMap<i64, StateGroupEntry>, state_group: i64) -> StateMap<Atom> {
    let mut entry = &map[&state_group];
    let mut state_map = StateMap::new();

    let mut stack = vec![state_group];

    while let Some(prev_state_group) = entry.prev_state_group {
        stack.push(prev_state_group);
        if !map.contains_key(&prev_state_group) {
            panic!("Missing {}", prev_state_group);
        }
        entry = &map[&prev_state_group];
    }

    for sg in stack.iter().rev() {
        state_map.extend(
            map[&sg]
                .state_map
                .iter()
                .map(|((t, s), e)| ((t, s), e.clone())),
        );
    }

    state_map
}

fn collapse_state_with_database(state_group: i64) -> StateMap<Atom> {
    // connect to the database
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let mut client = Client::connect(DB_URL, connector).unwrap();

    let mut entry: &StateGroupEntry;

    // Get's the delta for a specific state group
    let query_from_state_groups = r#"
        SELECT m.id, prev_state_group, type, state_key, s.event_id
        FROM state_groups AS m
        LEFT JOIN state_groups_state AS s ON (m.id = s.state_group)
        LEFT JOIN state_group_edges AS e ON (m.id = e.state_group)
        WHERE m.id = $1
    "#;

    // If there is no delta for that specific state group, then that doesn't neccessarily mean that 
    // there's an issue with the database - apparently it is OK for there to be states 
    let query_from_just_pred = r#"
        SELECT prev_state_group
        FROM state_group_edges 
        WHERE state_group = $1
    "#;

    StateMap::new()
}