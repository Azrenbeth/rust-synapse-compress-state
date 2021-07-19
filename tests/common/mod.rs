use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::Client;
use postgres_openssl::MakeTlsConnector;
use std::collections::BTreeMap;

use synapse_compress_state::{PGEscape, StateGroupEntry};

static DB_URL: &str = "postgresql://synapse_user:synapse_pass@localhost/synapse";

pub fn add_contents_to_database(room_id: &str, state_group_map: &BTreeMap<i64, StateGroupEntry>) {
    // connect to the database
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let mut client = Client::connect(DB_URL, connector).unwrap();

    let mut sql = "".to_string();

    for (sg, entry) in state_group_map {
        sql.push_str(&format!(
            "INSERT INTO state_groups (id, room_id, event_id) VALUES ({},{},{});\n",
            sg,
            PGEscape(room_id),
            PGEscape("left_blank")
        ));

        if let Some(prev_sg) = entry.prev_state_group {
            sql.push_str(&format!(
                "INSERT INTO state_group_edges (state_group, prev_state_group) VALUES ({}, {});\n",
                sg, prev_sg
            ));
        }

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

    println!("{}", sql);
    // client.execute(&sql[..], &[]).unwrap();
    client.batch_execute(&sql).unwrap();
}

pub fn empty_database() {
    // connect to the database
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let mut client = Client::connect(DB_URL, connector).unwrap();

    let mut sql = "".to_string();
    sql.push_str("DELETE FROM state_groups;\n");
    sql.push_str("DELETE FROM state_group_edges;\n");
    sql.push_str("DELETE FROM state_groups_state;\n");

    client.batch_execute(&sql).unwrap();
}
