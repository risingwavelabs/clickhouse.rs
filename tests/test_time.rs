#![cfg(feature = "time")]

use bytes::BufMut;
use bytes::BytesMut;
use clickhouse::update::Fileds;
use core::{assert_eq, time::Duration};
use std::ops::RangeBounds;
use std::thread::sleep;
use time::Month;

use rand::{distributions::Standard, Rng};
use serde::{Deserialize, Serialize};
use time::{macros::datetime, Date, OffsetDateTime};

use clickhouse::Row;

mod common;

#[common::named]
#[tokio::test]
async fn datetime() {
    let client = common::prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::time::datetime")]
        dt: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime::option")]
        dt_opt: Option<OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::secs")]
        dt64s: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::secs::option")]
        dt64s_opt: Option<OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::millis")]
        dt64ms: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::millis::option")]
        dt64ms_opt: Option<OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::micros")]
        dt64us: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::micros::option")]
        dt64us_opt: Option<OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
        dt64ns: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::nanos::option")]
        dt64ns_opt: Option<OffsetDateTime>,
    }

    #[derive(Debug, Deserialize, Row)]
    struct MyRowStr {
        dt: String,
        dt64s: String,
        dt64ms: String,
        dt64us: String,
        dt64ns: String,
    }

    client
        .query(
            "
            CREATE TABLE test(
                dt          DateTime,
                dt_opt      Nullable(DateTime),
                dt64s       DateTime64(0),
                dt64s_opt   Nullable(DateTime64(0)),
                dt64ms      DateTime64(3),
                dt64ms_opt  Nullable(DateTime64(3)),
                dt64us      DateTime64(6),
                dt64us_opt  Nullable(DateTime64(6)),
                dt64ns      DateTime64(9),
                dt64ns_opt  Nullable(DateTime64(9))
            )
            ENGINE = MergeTree ORDER BY dt
        ",
        )
        .execute()
        .await
        .unwrap();

    let original_row = MyRow {
        dt: datetime!(2022-11-13 15:27:42 UTC),
        dt_opt: Some(datetime!(2022-11-13 15:27:42 UTC)),
        dt64s: datetime!(2022-11-13 15:27:42 UTC),
        dt64s_opt: Some(datetime!(2022-11-13 15:27:42 UTC)),
        dt64ms: datetime!(2022-11-13 15:27:42.123 UTC),
        dt64ms_opt: Some(datetime!(2022-11-13 15:27:42.123 UTC)),
        dt64us: datetime!(2022-11-13 15:27:42.123456 UTC),
        dt64us_opt: Some(datetime!(2022-11-13 15:27:42.123456 UTC)),
        dt64ns: datetime!(2022-11-13 15:27:42.123456789 UTC),
        dt64ns_opt: Some(datetime!(2022-11-13 15:27:42.123456789 UTC)),
    };

    let mut insert = client.insert("test").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let row = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    let row_str = client
        .query(
            "
            SELECT toString(dt),
                   toString(dt64s),
                   toString(dt64ms),
                   toString(dt64us),
                   toString(dt64ns)
              FROM test
        ",
        )
        .fetch_one::<MyRowStr>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
    assert_eq!(row_str.dt, &original_row.dt.to_string()[..19]);
    assert_eq!(row_str.dt64s, &original_row.dt64s.to_string()[..19]);
    assert_eq!(row_str.dt64ms, &original_row.dt64ms.to_string()[..23]);
    assert_eq!(row_str.dt64us, &original_row.dt64us.to_string()[..26]);
    assert_eq!(row_str.dt64ns, &original_row.dt64ns.to_string()[..29]);
}

#[common::named]
#[tokio::test]
async fn date() {
    let client = common::prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::time::date")]
        date: Date,
        #[serde(with = "clickhouse::serde::time::date::option")]
        date_opt: Option<Date>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                date        Date,
                date_opt    Nullable(Date)
            ) ENGINE = MergeTree ORDER BY date
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert("test").unwrap();

    let dates = generate_dates(1970..2149, 100);
    for &date in &dates {
        let original_row = MyRow {
            date,
            date_opt: Some(date),
        };

        insert.write(&original_row).await.unwrap();
    }
    insert.end().await.unwrap();

    let actual = client
        .query("SELECT ?fields, toString(date) FROM test ORDER BY date")
        .fetch_all::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(actual.len(), dates.len());

    for ((row, date_str), expected) in actual.iter().zip(dates) {
        assert_eq!(row.date, expected);
        assert_eq!(row.date_opt, Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

#[common::named]
#[tokio::test]
async fn date32() {
    let client = common::prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::time::date32")]
        date: Date,
        #[serde(with = "clickhouse::serde::time::date32::option")]
        date_opt: Option<Date>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                date        Date32,
                date_opt    Nullable(Date32)
            ) ENGINE = MergeTree ORDER BY date
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert("test").unwrap();

    let dates = generate_dates(1925..2283, 100); // TODO: 1900..=2299 for newer versions.
    for &date in &dates {
        let original_row = MyRow {
            date,
            date_opt: Some(date),
        };

        insert.write(&original_row).await.unwrap();
    }
    insert.end().await.unwrap();

    let actual = client
        .query("SELECT ?fields, toString(date) FROM test ORDER BY date")
        .fetch_all::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(actual.len(), dates.len());

    for ((row, date_str), expected) in actual.iter().zip(dates) {
        assert_eq!(row.date, expected);
        assert_eq!(row.date_opt, Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

fn generate_dates(years: impl RangeBounds<i32>, count: usize) -> Vec<Date> {
    let mut rng = rand::thread_rng();
    let mut dates: Vec<_> = (&mut rng)
        .sample_iter(Standard)
        .filter(|date: &Date| years.contains(&date.year()))
        .take(count)
        .collect();

    dates.sort_unstable();
    dates
}

#[common::named]
#[tokio::test]
async fn test_insert_update_time() {
    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow {
        no: u32,
        #[serde(with = "clickhouse::serde::time::date32")]
        date: Date,
        #[serde(with = "clickhouse::serde::time::datetime")]
        dt: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::secs")]
        dt64s: OffsetDateTime,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE test(no UInt32 , date Date32 , dt DateTime , dt64s DateTime64(0))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .unwrap();

    // Write to the table.
    let mut insert = client.insert::<MyRow>("test").unwrap();
    let mut buffer = BytesMut::with_capacity(128 * 1024);
    buffer.put_i32_le(1);
    buffer.put_i32_le(13000);
    buffer.put_u32_le(1300000000);
    buffer.put_i64_le(1300000000);
    insert.write_row_binary(buffer).await.unwrap();
    insert.end().await.unwrap();

    sleep(Duration::from_secs(1));
    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow>()
        .unwrap();

    while let Some(row) = cursor.next().await.unwrap() {
        assert_eq!(
            row.date,
            Date::from_calendar_date(2005, Month::August, 5).unwrap()
        );
        assert_eq!(row.dt, datetime!(2011-03-13 7:06:40 UTC),);
        assert_eq!(row.dt64s, datetime!(2011-03-13 7:06:40 UTC),);
    }
    let update = client.update(
        "test",
        "no",
        vec![format!("date"), format!("dt"), format!("dt64s")],
    );
    let vec = vec![
        Fileds::Date(12000),
        Fileds::DateTime(1200000000),
        Fileds::DateTime64(1200000000),
    ];
    update.update_fileds(vec, 1 as u64).await.unwrap();
    sleep(Duration::from_secs(1));
    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow>()
        .unwrap();

    while let Some(row) = cursor.next().await.unwrap() {
        assert_eq!(
            row.date,
            Date::from_calendar_date(2002, Month::November, 9).unwrap()
        );
        assert_eq!(row.dt, datetime!(2008-01-10 21:20:00 UTC),);
        assert_eq!(row.dt64s, datetime!(2008-01-10 21:20:00 UTC),);
    }
}
