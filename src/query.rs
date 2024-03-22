use std::{cmp::Ordering, rc::Rc};

use sled::Db;

use crate::{docdb::DocDbError, query_executor::query_execute, query_planner::query_plan};

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
#[derive(Debug, Clone, PartialEq)]
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

// The ordering we define ignores the type of predicate --- E,
// GT and so on --- and instead focuses on the field and values.
// This is because, in the end, we want to order by field, then
// value, and then collapse down the predicates for each field
// into a single range scan.
impl PartialOrd for QP {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // These first two match statements generate
        // a pseudo-discriminant for the Enum, as
        // mem::discriminat's return value is only Eq not Ord.
        let s_d = match self {
            QP::E { .. } => 1,
            QP::GT { .. } => 2,
            QP::GTE { .. } => 3,
            QP::LT { .. } => 4,
            QP::LTE { .. } => 5,
        };
        let o_d = match other {
            QP::E { .. } => 1,
            QP::GT { .. } => 2,
            QP::GTE { .. } => 3,
            QP::LT { .. } => 4,
            QP::LTE { .. } => 5,
        };
        // Extract the path and value from self and other.
        let (s_p, s_v) = match self {
            QP::E { p, v }
            | QP::GT { p, v }
            | QP::GTE { p, v }
            | QP::LT { p, v }
            | QP::LTE { p, v } => (p, v),
        };
        let (o_p, o_v) = match other {
            QP::E { p, v }
            | QP::GT { p, v }
            | QP::GTE { p, v }
            | QP::LT { p, v }
            | QP::LTE { p, v } => (p, v),
        };
        // Finally, with that out the way, we can
        // order them.
        let ordp = s_p.partial_cmp(o_p)?;
        match ordp {
            Ordering::Less => Some(Ordering::Less),
            Ordering::Greater => Some(Ordering::Greater),
            Ordering::Equal => match s_v.partial_cmp(o_v)? {
                Ordering::Less => Some(Ordering::Less),
                Ordering::Greater => Some(Ordering::Greater),
                Ordering::Equal => Some(s_d.cmp(&o_d)),
            },
        }

        // self.height.partial_cmp(&other.height)
    }
}

pub type Query = Vec<QP>;
pub struct QueryStats {
    pub scans: u16,
}
pub struct QueryResult {
    pub results: Vec<String>,
    pub stats: QueryStats,
}

// Maybe we use this struct mapped to fields, or have two
// field -> key maps, one for start and one for end keys.
#[derive(Debug, PartialEq)]
pub(crate) struct Scan {
    pub(crate) skey: Vec<u8>,
    pub(crate) ekey: Vec<u8>,
}

pub fn search_index(db: &Db, q: Query) -> Result<QueryResult, DocDbError> {
    // I think Query here is a one-time use thing, so we should own it. Db
    // will be used again and again, so we should borrow it.
    let scans = query_plan(q)?;
    let query_result = query_execute(scans, db)?;
    Ok(query_result)
}

#[cfg(test)]
mod tests {
    use crate::query_executor::scan;
    use crate::query_planner::{
        scan_range_eq, scan_range_gt, scan_range_gte, scan_range_lt, scan_range_lte,
    };
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

        let (s, e) = scan_range_eq(keypath!["name"], tv("john"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        let (s, e) = scan_range_eq(keypath!["a", "b"], tv(1));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc1"], ids);
        let (s, e) = scan_range_eq(keypath!["a", "b"], tv(2));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(Vec::<String>::new(), ids);
        let (s, e) = scan_range_eq(keypath!["a", "c"], tv(2));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3"], ids);

        Ok(())
    }
    #[test]
    fn lookup_gte_test() -> Result<(), DocDbError> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        insert_test_data(&db)?;

        let (s, e) = scan_range_gte(keypath!["age"], tv(25));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc1", "doc3"], ids);
        let (s, e) = scan_range_gte(keypath!["name"], tv("mi"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc1"], ids);
        // Expected IDs are sorted in index order intentionally
        let (s, e) = scan_range_gte(keypath!["name"], tv("john"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let (s, e) = scan_range_gte(keypath!["name"], tv(100_000_000));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let (s, e) = scan_range_gte(keypath!["name"], tv(false));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let (s, e) = scan_range_gte(keypath!["name"], tv(true));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let (s, e) = scan_range_gte(keypath!["name"], tv("azzzzzzzzz"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);

        let (s, e) = scan_range_gte(keypath!["age"], tv("a"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(Vec::<String>::new(), ids);
        let (s, e) = scan_range_gte(keypath!["age"], tv(false));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);
        let (s, e) = scan_range_gte(keypath!["age"], tv(true));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);

        Ok(())
    }

    #[test]
    fn lookup_gt_test() -> Result<(), DocDbError> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        insert_test_data(&db)?;

        let (s, e) = scan_range_gt(keypath!["age"], tv(24));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc1", "doc3"], ids);
        let (s, e) = scan_range_gt(keypath!["name"], tv("mi"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc1"], ids);
        // Expected IDs are sorted in index order intentionally
        let (s, e) = scan_range_gt(keypath!["name"], tv("john"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc1"], ids);
        let (s, e) = scan_range_gt(keypath!["name"], tv(100_000_000));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let (s, e) = scan_range_gt(keypath!["name"], tv(false));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let (s, e) = scan_range_gt(keypath!["name"], tv(true));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);
        let (s, e) = scan_range_gt(keypath!["name"], tv("azzzzzzzzz"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);

        let (s, e) = scan_range_gt(keypath!["age"], tv("a"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(Vec::<String>::new(), ids);
        let (s, e) = scan_range_gt(keypath!["age"], tv(false));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);
        let (s, e) = scan_range_gt(keypath!["age"], tv(true));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);

        Ok(())
    }

    #[test]
    fn lookup_lt_test() -> Result<(), DocDbError> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        insert_test_data(&db)?;

        let (s, e) = scan_range_lt(keypath!["age"], tv(40));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2"], ids);
        let (s, e) = scan_range_lt(keypath!["name"], tv("mi"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        // Expected IDs are sorted in index order intentionally
        let (s, e) = scan_range_lt(keypath!["name"], tv("johna"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        let (s, e) = scan_range_lt(keypath!["name"], tv("zaaaaaaaaaa"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);

        let (s, e) = scan_range_lt(keypath!["age"], tv("a"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);
        let (s, e) = scan_range_lt(keypath!["age"], tv(false));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(Vec::<String>::new(), ids);
        let (s, e) = scan_range_lt(keypath!["age"], tv(true));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(Vec::<String>::new(), ids);

        Ok(())
    }

    #[test]
    fn lookup_lte_test() -> Result<(), DocDbError> {
        let tmp_dir = tempdir().unwrap();
        let db = docdb::new_database(tmp_dir.path()).unwrap();
        insert_test_data(&db)?;

        let (s, e) = scan_range_lte(keypath!["age"], tv(40));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc1"], ids);
        let (s, e) = scan_range_lte(keypath!["name"], tv("mi"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        // Expected IDs are sorted in index order intentionally
        let (s, e) = scan_range_lte(keypath!["name"], tv("johna"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3"], ids);
        let (s, e) = scan_range_lte(keypath!["name"], tv("zaaaaaaaaaa"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc3", "doc1"], ids);

        let (s, e) = scan_range_lte(keypath!["age"], tv("a"));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(vec!["doc2", "doc1", "doc3"], ids);
        let (s, e) = scan_range_lte(keypath!["age"], tv(false));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(Vec::<String>::new(), ids);
        let (s, e) = scan_range_lte(keypath!["age"], tv(true));
        let ids = scan(&db, &s, &e)?;
        assert_eq!(Vec::<String>::new(), ids);

        Ok(())
    }

    #[test]
    fn qp_ordering() {
        // Queries are ordered without regard for their predicate
        // type, instead only by their path and then value. Finally,
        // if those are equal, their type.
        let mut rng = thread_rng();
        let expected = vec![
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
            QP::GTE {
                p: keypath!("quux"),
                v: tv(99),
            },
            QP::LTE {
                p: keypath!("quux"),
                v: tv("last"),
            },
            QP::E {
                p: keypath!("quux", "bar"),
                v: tv(1),
            },
            QP::GTE {
                p: keypath!("quux", "bar", "baz"),
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

    fn query_sort(q: &mut Query) {
        q.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }
}
