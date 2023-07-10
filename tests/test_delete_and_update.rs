use core::time::Duration;
use std::{collections::HashSet, thread::sleep};

use serde::{Deserialize, Serialize};

use clickhouse::{update::Fileds, Row};

mod common;

#[common::named]
#[tokio::test]
async fn commod() {
    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        name: &'a str,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE test(no UInt32, name LowCardinality(String))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .unwrap();

    // Write to the table.
    let mut insert = client.insert("test").unwrap();
    for i in 0..1000 {
        insert.write(&MyRow { no: i, name: "foo" }).await.unwrap();
    }

    insert.end().await.unwrap();

    let mut pk_vec = vec![];
    for i in 0..10 {
        pk_vec.push(i as u64)
    }
    pk_vec.push(567 as u64);
    pk_vec.push(545 as u64);
    pk_vec.push(674 as u64);
    pk_vec.push(873 as u64);

    let set: HashSet<u64> = pk_vec.iter().map(|x| *x).collect();
    let delete = client.delete("test", "no", pk_vec);
    delete.delete().await.unwrap();
    sleep(Duration::from_secs(1));
    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow<'_>>()
        .unwrap();

    while let Some(row) = cursor.next().await.unwrap() {
        assert!(!set.contains(&(row.no as u64)));
    }

    for i in 700..750 {
        let update = client.update("test", "no", vec![format!("name")]);
        let vec = vec![Fileds::String(format!("name"))];
        update.update_fileds(vec, i as u64).await.unwrap();
    }
    sleep(Duration::from_secs(1));

    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow<'_>>()
        .unwrap();

    while let Some(row) = cursor.next().await.unwrap() {
        if row.no >= 700 && row.no < 750 {
            assert_eq!(row.name, "name");
        } else {
            assert_eq!(row.name, "foo");
        }
    }
}
