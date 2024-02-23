use std::{collections::BTreeMap, rc::Rc};

use sled::Db;

use crate::encoding::{self};

#[derive(Clone, Debug, PartialEq)]
pub enum TaggableValue {
    Null,
    Bool(bool),
    String(String),
    RcString(Rc<String>), // Rc<String> avoids cloning field name string buffers
    // ArrayIndex(usize), // Can we encode a usize more easily?
    Number(f64),
}

pub fn tv<T: Into<TaggableValue>>(v: T) -> TaggableValue {
    v.into()
}

// impl From<null> for TaggableValue {
//     fn from(value: null) -> Self {
//         TaggableValue::Null
//     }
// }

impl From<bool> for TaggableValue {
    fn from(value: bool) -> Self {
        TaggableValue::Bool(value)
    }
}

// Shortens TaggableValue::String(str.to_string())
impl From<&str> for TaggableValue {
    fn from(s: &str) -> Self {
        TaggableValue::String(s.to_string())
    }
}
// Shortens TaggableValue::Number(1.0) or TaggableValue(int as f64)
impl From<i64> for TaggableValue {
    fn from(i: i64) -> Self {
        TaggableValue::Number(i as f64)
    }
}

impl From<f64> for TaggableValue {
    fn from(value: f64) -> Self {
        TaggableValue::Number(value)
    }
}

impl From<String> for TaggableValue {
    fn from(value: String) -> Self {
        TaggableValue::String(value)
    }
}

impl From<Rc<String>> for TaggableValue {
    fn from(value: Rc<String>) -> Self {
        TaggableValue::RcString(value)
    }
}

// TODO ergonomically TaggableValue shouldn't be a thing
// that external users see. But we do need to restrict the
// types that users can put into it.
// maybe we should have `into` from a lot of things and the
// generic thing here is <T: Into<TaggableValue>>
// But we can't have generics in the enum definition.

// QP is a query predicate. A query is a list of
// QPs that are ANDed together.
pub enum QP<'a> {
    E { p: Vec<&'a str>, v: TaggableValue },
    // GT {
    //     p: Vec<TaggableValue>,
    //     v: TaggableValue,
    // },
    // GTE {
    //     p: Vec<TaggableValue>,
    //     v: TaggableValue,
    // },
    // LT {
    //     p: Vec<TaggableValue>,
    //     v: TaggableValue,
    // },
    // LTE {
    //     p: Vec<TaggableValue>,
    //     v: TaggableValue,
    // },
}

pub type Query<'a> = Vec<QP<'a>>;

pub fn search_index(db: &Db, q: Query) -> Result<Vec<String>, sled::Error> {
    // I think Query here is a one-time use thing, so we should own it. Db
    // will be used again and again, so we should borrow it.
    let mut result_ids = BTreeMap::new();
    let mut n_preds = 0;

    for qp in q {
        n_preds += 1;
        match qp {
            QP::E { p, v } => {
                let ids = lookup_eq(db, p, v)?;
                for id in ids {
                    let count = result_ids.entry(id).or_insert(0);
                    *count += 1;
                }
            }
        }
    }

    let mut result = vec![];
    for (id, n) in result_ids {
        if n == n_preds {
            result.push(id);
        }
    }

    Ok(result)
}

fn lookup_eq(db: &Db, path: Vec<&str>, v: TaggableValue) -> Result<Vec<String>, sled::Error> {
    let mut ids = vec![];
    let start_key = encoding::encode_index_query_start_key(&path, &v);
    let end_key = encoding::encode_index_query_end_key(&path, &v);

    let iter = db.range(start_key..end_key);
    for i in iter {
        let (k, _) = i?;
        match encoding::decode_index_key_docid(&k) {
            Ok(v) => ids.push(v.to_string()),
            Err(_) => println!("Couldn't decode docID from {:?}", &k),
        };
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use crate::docdb;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn lookup_eq_test() -> Result<(), sled::Error> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        docdb::insert_document(
            &db,
            "doc1",
            json!({"a":{"b": 1}, "name": "mike", "age": 40}),
        )?;
        docdb::insert_document(
            &db,
            "doc2",
            json!({"a":{"c": 2}, "name": "john", "age": 24}),
        )?;
        docdb::insert_document(
            &db,
            "doc3",
            json!({"a":{"c": 2}, "name": "john", "age": 110}),
        )?;

        let ids = lookup_eq(&db, vec!["name"], tv("john"))?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        let ids = lookup_eq(&db, vec!["a", "b"], tv(1))?;
        assert_eq!(vec!["doc1"], ids);
        let ids = lookup_eq(&db, vec!["a", "b"], tv(2))?;
        assert_eq!(Vec::<String>::new(), ids);
        let ids = lookup_eq(&db, vec!["a", "c"], tv(2))?;
        assert_eq!(vec!["doc2", "doc3"], ids);

        Ok(())
    }
}
