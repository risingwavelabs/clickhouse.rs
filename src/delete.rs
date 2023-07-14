use crate::{error::Result, query::Query, Client, update::Fileds};

#[must_use]
#[derive(Clone)]
pub struct Delete {
    query: Query,
    delete_pk: Vec<Fileds>,
}

impl Delete {
    pub(crate) fn new(
        client: &Client,
        table_name: &str,
        pk_name: &str,
        delete_pk: Vec<Fileds>,
    ) -> Self {
        let mut out = delete_pk.iter().enumerate().fold(
            format!("ALTER TABLE {table_name} DELETE WHERE {pk_name} in ("),
            |mut res, (idx, _pk)| {
                if idx > 0 {
                    res.push(',');
                }
                res.push_str("?");
                res
            },
        );
        out.push_str(")");
        let query = Query::new(client, &out);
        Self { query, delete_pk }
    }
    pub async fn delete(mut self) -> Result<()> {
        self.delete_pk.clone().iter().for_each(|a| {
            a.bind_fields(&mut self.query);
        });
        self.query.execute().await
    }
}
