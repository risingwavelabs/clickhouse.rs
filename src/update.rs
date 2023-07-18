use crate::{error::Result, query::Query, Client};

#[must_use]
#[derive(Clone)]
pub struct Update {
    query: Query,
}

impl Update {
    pub(crate) fn new(
        client: &Client,
        table_name: &str,
        pk_name: &str,
        flieds_names: Vec<String>,
    ) -> Self {
        let mut out: String = flieds_names.iter().enumerate().fold(
            format!("ALTER TABLE {table_name} UPDATE"),
            |mut res, (idx, key)| {
                if idx > 0 {
                    res.push(',');
                }
                res.push_str(&format!(" {key} = ?"));
                res
            },
        );
        out.push_str(&format!(" where {pk_name} = ?"));
        let query = Query::new(client, &out);
        Self { query }
    }
    pub async fn update_fields(mut self, fields: Vec<Field>, pk: Field) -> Result<()> {
        fields.iter().for_each(|a| {
            a.bind_fields(&mut self.query);
        });
        pk.bind_fields(&mut self.query);
        self.query.execute().await
    }
}
#[derive(Clone)]
pub enum Field {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(f32),
    F64(f64),
    Char(char),
    String(String),
    Vec(Vec<Field>),
    Date(u16),
    Date32(i32),
    DateTime(u32),
    DateTime64(i64),
    Customize(String),
}
impl Field {
    pub fn bind_fields(&self, query: &mut Query) {
        match self {
            Field::Bool(v) => query.bind_ref(v),
            Field::I8(v) => query.bind_ref(v),
            Field::I16(v) => query.bind_ref(v),
            Field::I32(v) => query.bind_ref(v),
            Field::I64(v) => query.bind_ref(v),
            Field::I128(v) => query.bind_ref(v),
            Field::U8(v) => query.bind_ref(v),
            Field::U16(v) => query.bind_ref(v),
            Field::U32(v) => query.bind_ref(v),
            Field::U64(v) => query.bind_ref(v),
            Field::U128(v) => query.bind_ref(v),
            Field::F32(v) => query.bind_ref(v),
            Field::F64(v) => query.bind_ref(v),
            Field::Char(v) => query.bind_ref(v),
            Field::String(v) => query.bind_ref(v),
            Field::Vec(_) => {
                todo!()
            }
            Field::Customize(v) => {
                query.bind_str(v);
            }
            Field::Date(v) => query.bind_ref(v),
            Field::Date32(v) => query.bind_ref(v),
            Field::DateTime(v) => query.bind_ref(v),
            Field::DateTime64(v) => query.bind_ref(v),
        };
    }
}
