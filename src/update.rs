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
    pub async fn update_fileds(mut self, fileds: Vec<Fileds>, pk_name: u64) -> Result<()> {
        fileds.iter().for_each(|a| {
            a.bind_fields(&mut self.query);
        });
        self.query.bind_ref(pk_name);
        self.query.execute().await
    }
}
#[derive(Clone)]
pub enum Fileds {
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
    Vec(Vec<Fileds>),
    Date(u16),
    Date32(i32),
    DateTime(u32),
    DateTime64(i64),
    Str(String),
}
impl Fileds {
    pub fn bind_fields(&self,query:&mut Query){
        match self {
            Fileds::Bool(v) => query.bind_ref(v),
            Fileds::I8(v) => query.bind_ref(v),
            Fileds::I16(v) => query.bind_ref(v),
            Fileds::I32(v) => query.bind_ref(v),
            Fileds::I64(v) => query.bind_ref(v),
            Fileds::I128(v) => query.bind_ref(v),
            Fileds::U8(v) => query.bind_ref(v),
            Fileds::U16(v) => query.bind_ref(v),
            Fileds::U32(v) => query.bind_ref(v),
            Fileds::U64(v) => query.bind_ref(v),
            Fileds::U128(v) => query.bind_ref(v),
            Fileds::F32(v) => query.bind_ref(v),
            Fileds::F64(v) => query.bind_ref(v),
            Fileds::Char(v) => query.bind_ref(v),
            Fileds::String(v) => query.bind_ref(v),
            Fileds::Vec(_) => {
                todo!()
            }
            Fileds::Str(v) => {
                query.bind_str(v);
            }
            Fileds::Date(v) => query.bind_ref(v),
            Fileds::Date32(v) => query.bind_ref(v),
            Fileds::DateTime(v) => query.bind_ref(v),
            Fileds::DateTime64(v) => query.bind_ref(v),
        };
    }
}
