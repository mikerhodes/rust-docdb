use std::{collections::BTreeMap, rc::Rc};

use sled::Db;

use crate::{
    docdb::DocDbError,
    encoding::{self},
};

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum TaggableValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    RcString(Rc<String>), // Rc<String> avoids cloning field name string buffers
                          // ArrayIndex(usize), // Can we encode a usize more easily?
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
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum QP {
    E {
        p: Vec<TaggableValue>,
        v: TaggableValue,
    },
    GT {
        p: Vec<TaggableValue>,
        v: TaggableValue,
    },
    GTE {
        p: Vec<TaggableValue>,
        v: TaggableValue,
    },
    LT {
        p: Vec<TaggableValue>,
        v: TaggableValue,
    },
    LTE {
        p: Vec<TaggableValue>,
        v: TaggableValue,
    },
}

pub type Query = Vec<QP>;
pub struct QueryStats {
    pub scans: u16,
}
pub struct QueryResult {
    pub results: Vec<String>,
    pub stats: QueryStats,
}

fn query_sort(q: &mut Query) {
    q.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
}

pub fn search_index(db: &Db, mut q: Query) -> Result<QueryResult, DocDbError> {
    // I think Query here is a one-time use thing, so we should own it. Db
    // will be used again and again, so we should borrow it.

    // BTreeMap so we return IDs to caller in order
    let mut result_ids = BTreeMap::new();
    let mut n_preds = 0;
    let mut stats = QueryStats { scans: 0 };
    let mut first_predicate = true;

    // TODO convert all the predicates into a set of
    // range scans, Scan { startkey, endkey }
    // then we can later collapse the ranges for the same
    // keys (using the IndexKey path_prefix method to group
    // by the paths).

    // Sort by the ordering in the enum, which puts equality
    // first, which is likely to have a smaller result set
    // than any range query. This means we likely end up using
    // a little less memory because of only storing the IDs that
    // are returned by the first predicate. Not sure if this sorts by the
    // path and value, as TaggableValue doesn't implement Ord.
    // https://stackoverflow.com/a/70588789
    query_sort(&mut q);

    // TODO the problem right now is that query_sort doesn't sort by field
    // first, it sorts by predicate type (ie, enum definition order).
    // We might have to write a custom ordering method, well perhaps
    // query_sort can use sort_by to make it work.

    // I think what we want to do is to get lookup_* to return
    // the start/end key pairs. We can then map the sorted query
    // to a set of (field, start/end key). Once we have that, then
    // we can do the work to loop over the query, and for each
    // field, generate a new query that collapses the start/end
    // key for each field into a single entry --- or, if at least
    // one field has non-overlapping ranges, return an error so
    // we can exit the AND.

    for qp in q {
        n_preds += 1;
        let ids = match qp {
            QP::E { p, v } => lookup_eq(db, p, v)?,
            QP::GT { p, v } => lookup_gt(db, p, v)?,
            QP::GTE { p, v } => lookup_gte(db, p, v)?,
            QP::LT { p, v } => lookup_lt(db, p, v)?,
            QP::LTE { p, v } => lookup_lte(db, p, v)?,
        };
        stats.scans += 1;

        if ids.is_empty() {
            // Short-circuit evaluation; an empty result set means
            // this conjunction can't have any results. Stop scanning.
            return Ok(QueryResult {
                results: vec![],
                stats,
            });
        }

        for id in ids {
            let e = result_ids.entry(id).and_modify(|c| *c += 1);
            if first_predicate {
                // As no result ID that appears in a later scan but not the
                // first scan can be in the final result set, don't use memory
                // to store them.
                e.or_insert(1);
            }
        }

        first_predicate = false;
    }

    // Only those entries which were found in every index scan
    // should be in the final result set.
    let mut results = vec![];
    for (id, n) in result_ids {
        if n == n_preds {
            results.push(id);
        }
    }

    Ok(QueryResult { results, stats })
}

fn lookup_eq(
    db: &Db,
    path: Vec<TaggableValue>,
    v: TaggableValue,
) -> Result<Vec<String>, DocDbError> {
    let start_key = encoding::encode_index_query_pv_start_key(&path, &v);
    let end_key = encoding::encode_index_query_pv_end_key(&path, &v);
    scan(&db, &start_key, &end_key)
}

fn lookup_gte(
    db: &Db,
    path: Vec<TaggableValue>,
    v: TaggableValue,
) -> Result<Vec<String>, DocDbError> {
    let start_key = encoding::encode_index_query_pv_start_key(&path, &v);
    let end_key = encoding::encode_index_query_p_end_key(&path);
    scan(&db, &start_key, &end_key)
}

fn lookup_gt(
    db: &Db,
    path: Vec<TaggableValue>,
    v: TaggableValue,
) -> Result<Vec<String>, DocDbError> {
    let start_key = encoding::encode_index_query_pv_end_key(&path, &v);
    let end_key = encoding::encode_index_query_p_end_key(&path);
    scan(&db, &start_key, &end_key)
}

fn lookup_lt(
    db: &Db,
    path: Vec<TaggableValue>,
    v: TaggableValue,
) -> Result<Vec<String>, DocDbError> {
    let start_key = encoding::encode_index_query_p_start_key(&path);
    let end_key = encoding::encode_index_query_pv_start_key(&path, &v);
    scan(&db, &start_key, &end_key)
}

fn lookup_lte(
    db: &Db,
    path: Vec<TaggableValue>,
    v: TaggableValue,
) -> Result<Vec<String>, DocDbError> {
    let start_key = encoding::encode_index_query_p_start_key(&path);
    let end_key = encoding::encode_index_query_pv_end_key(&path, &v);
    scan(&db, &start_key, &end_key)
}

fn scan(db: &Db, start_key: &[u8], end_key: &[u8]) -> Result<Vec<String>, DocDbError> {
    let mut ids = vec![];
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
    use crate::{docdb, keypath};
    use serde_json::json;
    use tempfile::tempdir;

    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use super::*;

    fn insert_test_data(db: &Db) -> Result<(), DocDbError> {
        docdb::set_document(
            &db,
            "doc1",
            json!({"a":{"b": 1}, "name": "mike", "age": 40}),
        )?;
        docdb::set_document(
            &db,
            "doc2",
            json!({"a":{"c": 2}, "name": "john", "age": 24}),
        )?;
        docdb::set_document(
            &db,
            "doc3",
            json!({"a":{"c": 2}, "name": "john", "age": 110}),
        )?;
        Ok(())
    }

    #[test]
    fn lookup_eq_test() -> Result<(), DocDbError> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        insert_test_data(&db)?;

        let ids = lookup_eq(&db, keypath!["name"], tv("john"))?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        let ids = lookup_eq(&db, keypath!["a", "b"], tv(1))?;
        assert_eq!(vec!["doc1"], ids);
        let ids = lookup_eq(&db, keypath!["a", "b"], tv(2))?;
        assert_eq!(Vec::<String>::new(), ids);
        let ids = lookup_eq(&db, keypath!["a", "c"], tv(2))?;
        assert_eq!(vec!["doc2", "doc3"], ids);

        Ok(())
    }
    #[test]
    fn lookup_gte_test() -> Result<(), DocDbError> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        insert_test_data(&db)?;

        let ids = lookup_gte(&db, keypath!["age"], tv(25))?;
        assert_eq!(vec!["doc1", "doc3"], ids);
        let ids = lookup_gte(&db, keypath!["name"], tv("mi"))?;
        assert_eq!(vec!["doc1"], ids);
        // Expected IDs are sorted in index order intentionally
        let ids = lookup_gte(&db, keypath!["name"], tv("john"))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let ids = lookup_gte(&db, keypath!["name"], tv(100_000_000))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let ids = lookup_gte(&db, keypath!["name"], tv(false))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let ids = lookup_gte(&db, keypath!["name"], tv(true))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let ids = lookup_gte(&db, keypath!["name"], tv("azzzzzzzzz"))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);

        let ids = lookup_gte(&db, keypath!["age"], tv("a"))?;
        assert_eq!(Vec::<String>::new(), ids);
        let ids = lookup_gte(&db, keypath!["age"], tv(false))?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);
        let ids = lookup_gte(&db, keypath!["age"], tv(true))?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);

        Ok(())
    }

    #[test]
    fn lookup_gt_test() -> Result<(), DocDbError> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        insert_test_data(&db)?;

        let ids = lookup_gt(&db, keypath!["age"], tv(24))?;
        assert_eq!(vec!["doc1", "doc3"], ids);
        let ids = lookup_gt(&db, keypath!["name"], tv("mi"))?;
        assert_eq!(vec!["doc1"], ids);
        // Expected IDs are sorted in index order intentionally
        let ids = lookup_gt(&db, keypath!["name"], tv("john"))?;
        assert_eq!(vec!["doc1"], ids);
        let ids = lookup_gt(&db, keypath!["name"], tv(100_000_000))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let ids = lookup_gt(&db, keypath!["name"], tv(false))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let ids = lookup_gt(&db, keypath!["name"], tv(true))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let ids = lookup_gt(&db, keypath!["name"], tv("azzzzzzzzz"))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);

        let ids = lookup_gt(&db, keypath!["age"], tv("a"))?;
        assert_eq!(Vec::<String>::new(), ids);
        let ids = lookup_gt(&db, keypath!["age"], tv(false))?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);
        let ids = lookup_gt(&db, keypath!["age"], tv(true))?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);

        Ok(())
    }

    #[test]
    fn lookup_lt_test() -> Result<(), DocDbError> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        insert_test_data(&db)?;

        let ids = lookup_lt(&db, keypath!["age"], tv(40))?;
        assert_eq!(vec!["doc2"], ids);
        let ids = lookup_lt(&db, keypath!["name"], tv("mi"))?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        // Expected IDs are sorted in index order intentionally
        let ids = lookup_lt(&db, keypath!["name"], tv("johna"))?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        let ids = lookup_lt(&db, keypath!["name"], tv("zaaaaaaaaaa"))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);

        let ids = lookup_lt(&db, keypath!["age"], tv("a"))?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);
        let ids = lookup_lt(&db, keypath!["age"], tv(false))?;
        assert_eq!(Vec::<String>::new(), ids);
        let ids = lookup_lt(&db, keypath!["age"], tv(true))?;
        assert_eq!(Vec::<String>::new(), ids);

        Ok(())
    }

    #[test]
    fn lookup_lte_test() -> Result<(), DocDbError> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        insert_test_data(&db)?;

        let ids = lookup_lte(&db, keypath!["age"], tv(40))?;
        assert_eq!(vec!["doc2", "doc1"], ids);
        let ids = lookup_lte(&db, keypath!["name"], tv("mi"))?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        // Expected IDs are sorted in index order intentionally
        let ids = lookup_lte(&db, keypath!["name"], tv("johna"))?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        let ids = lookup_lte(&db, keypath!["name"], tv("zaaaaaaaaaa"))?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);

        let ids = lookup_lte(&db, keypath!["age"], tv("a"))?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);
        let ids = lookup_lte(&db, keypath!["age"], tv(false))?;
        assert_eq!(Vec::<String>::new(), ids);
        let ids = lookup_lte(&db, keypath!["age"], tv(true))?;
        assert_eq!(Vec::<String>::new(), ids);

        Ok(())
    }

    #[test]
    fn qp_ordering() {
        let mut rng = thread_rng();
        let expected = vec![
            QP::E {
                p: keypath!("foo", "bar"),
                v: tv(11),
            },
            QP::E {
                p: keypath!("foo", "bar"),
                v: tv(111),
            },
            QP::E {
                p: keypath!("quux"),
                v: tv(false),
            },
            QP::E {
                p: keypath!("quux"),
                v: tv(1),
            },
            QP::E {
                p: keypath!("quux"),
                v: tv(99),
            },
            QP::E {
                p: keypath!("quux"),
                v: tv("last"),
            },
            QP::E {
                p: keypath!("quux", "bar"),
                v: tv(1),
            },
            QP::E {
                p: keypath!("quux", "bar", "baz"),
                v: tv(1),
            },
            QP::GT {
                p: keypath!("foo", "bar"),
                v: tv(1),
            },
            QP::GTE {
                p: keypath!("foo", "bar"),
                v: tv(1),
            },
            QP::LT {
                p: keypath!("foo", "bar"),
                v: tv(1),
            },
            QP::LT {
                p: keypath!("foo", "bar"),
                v: tv(1),
            },
            QP::LT {
                p: keypath!("foo", "bar"),
                v: tv(1),
            },
            QP::LTE {
                p: keypath!("foo", "bar"),
                v: tv(1),
            },
        ];

        for _ in 1..1001 {
            let mut test = expected.clone();
            test.shuffle(&mut rng);
            query_sort(&mut test);
            assert_eq!(expected, test);
        }
    }
}
