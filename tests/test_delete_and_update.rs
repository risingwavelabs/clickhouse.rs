use core::time::Duration;
use std::{collections::HashSet, thread::sleep};

use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};

use clickhouse::{update::Fileds, Row};

mod common;

#[common::named]
#[tokio::test]
async fn test_update_delete() {
    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        name: &'a str,
        list: Vec<i32>,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE test(no UInt32, name LowCardinality(String) , list Array(UInt32))
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
        insert
            .write(&MyRow {
                no: i,
                name: "foo",
                list: vec![1, 2, 3],
            })
            .await
            .unwrap();
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
        let update = client.update("test", "no", vec![format!("name"), format!("list")]);
        let vec = vec![
            Fileds::String(format!("name")),
            Fileds::Str(format!("[2,5,8]")),
        ];
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
            assert_eq!(row.list, vec![2, 5, 8]);
        } else {
            assert_eq!(row.name, "foo");
            assert_eq!(row.list, vec![1, 2, 3]);
        }
    }
}

#[common::named]
#[tokio::test]
async fn test_insert() {
    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow {
        no: u32,
        date: Vec<i32>,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE test(no UInt32, date Array(UInt32))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .unwrap();

    // Write to the table.
    let mut insert = client.insert::<MyRow>("test").unwrap();
    for i in 0..10 {
        let vec = vec![1, 2, 3, 4];
        let mut buffer = BytesMut::with_capacity(128 * 1024);
        buffer.put_i32_le(i);
        put_unsigned_leb128(&mut buffer, vec.len() as u64);
        vec.iter().for_each(|&v| {
            buffer.put_i32_le(v);
        });
        insert.write_row_binary(buffer).await.unwrap();
    }
    insert.end().await.unwrap();

    sleep(Duration::from_secs(1));
    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow>()
        .unwrap();

    while let Some(row) = cursor.next().await.unwrap() {
        println!("row{:?}", row);
    }
}
pub fn put_unsigned_leb128(mut buffer: impl BufMut, mut value: u64) {
    while {
        let mut byte = value as u8 & 0x7f;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);

        value != 0
    } {}
}
