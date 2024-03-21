use rust_docdb::docdb;
use rust_docdb::docdb::DocDbError;
use rust_docdb::keypath;
use rust_docdb::query;
use rust_docdb::query::tv;
use rust_docdb::query::TaggableValue;
use serde_json::json;
use sled;
use sled::Db;
use tempfile::tempdir;

fn insert_test_data(db: &Db) -> Result<(), DocDbError> {
    docdb::set_document(
        &db,
        "doc1",
        json!({"a":{"b": 1}, "name": "mike", "age": 40, "pet": ["cat", "cat", "dog"]}),
    )?;
    docdb::set_document(
        &db,
        "doc2",
        json!({"a":{"c": 2}, "name": "john", "age": 24}),
    )?;
    docdb::set_document(
        &db,
        "doc3",
        json!({"a":{"c": 2}, "name": "john", "age": 110, "pet": ["wombat"]}),
    )?;
    Ok(())
}

#[test]
fn query_eq() -> Result<(), DocDbError> {
    let tmp_dir = tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    insert_test_data(&db)?;

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["name"],
            v: tv("john"),
        }],
    )?;
    assert_eq!(vec!["doc2".to_string(), "doc3".to_string()], ids.results);

    let ids = query::search_index(
        &db,
        vec![
            query::QP::E {
                p: keypath!["name"],
                v: tv("john"),
            },
            query::QP::E {
                p: keypath!["age"],
                v: tv(110),
            },
            query::QP::E {
                p: keypath!["a", "c"],
                v: tv(2),
            },
        ],
    )?;
    assert_eq!(vec!["doc3".to_string()], ids.results);

    let ids = query::search_index(
        &db,
        vec![
            query::QP::E {
                p: keypath!["name"],
                v: tv("john"),
            },
            query::QP::E {
                p: keypath!["age"],
                v: tv(110),
            },
            query::QP::E {
                p: keypath!["a", "c"],
                v: tv(1), // this results in no matches
            },
        ],
    )?;
    assert_eq!(Vec::<String>::new(), ids.results);

    Ok(())
}

#[test]
fn query_array_eq() -> Result<(), DocDbError> {
    let tmp_dir = tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    insert_test_data(&db)?;

    docdb::set_document(
        &db,
        "arrayed",
        json!({"arrs": [{"animals": ["cat", "wombat", "possum"]}, {"animals": "shark", "nums": [1,2,3,4,5]}]}),
    )?;
    docdb::set_document(
        &db,
        "arrayed2",
        json!({"arrs": [{"animals": "shark"}, {"animals": "shark", "nums": [1,2,3,4,5]}]}),
    )?;

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 0, "animals", 0],
            v: tv("cat"),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string()], ids.results);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 0, "animals", 2],
            v: tv("possum"),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string()], ids.results);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 1, "animals", 2],
            v: tv("possum"),
        }],
    )?;
    assert_eq!(Vec::<String>::new(), ids.results);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 0, "animals"],
            v: tv("shark"),
        }],
    )?;
    assert_eq!(vec!["arrayed2".to_string()], ids.results);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 1, "nums", 2],
            v: tv(3),
        }],
    )?;
    assert_eq!(
        vec!["arrayed".to_string(), "arrayed2".to_string()],
        ids.results
    );

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 0, "nums", 2],
            v: tv(3),
        }],
    )?;
    assert_eq!(Vec::<String>::new(), ids.results);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 0, "animals"], // check that we can change schema from array to string
            v: tv("shark"),
        }],
    )?;
    assert_eq!(vec!["arrayed2".to_string()], ids.results);

    Ok(())
}

#[test]
fn query_gte() -> Result<(), DocDbError> {
    let tmp_dir = tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    insert_test_data(&db)?;

    let ids = query::search_index(
        &db,
        vec![query::QP::GTE {
            p: keypath!["name"],
            v: tv("john"),
        }],
    )?;
    assert_eq!(
        vec!["doc1".to_string(), "doc2".to_string(), "doc3".to_string()],
        ids.results
    );
    let ids = query::search_index(
        &db,
        vec![
            query::QP::GTE {
                p: keypath!["name"],
                v: tv("john"),
            },
            query::QP::GTE {
                p: keypath!["age"],
                v: tv(50),
            },
        ],
    )?;
    assert_eq!(vec!["doc3".to_string()], ids.results);

    docdb::set_document(&db, "arrayed", json!({"arr": [1,2,"foo",4]}))?;
    let ids = query::search_index(
        &db,
        vec![query::QP::GTE {
            p: keypath!["arr", 2],
            v: tv(-1),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string()], ids.results);
    let ids = query::search_index(
        &db,
        vec![query::QP::GTE {
            p: keypath!["arr", 2],
            v: tv(-1),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string()], ids.results);
    let ids = query::search_index(
        &db,
        vec![query::QP::GTE {
            p: keypath!["arr", 2],
            v: tv("bar"),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string()], ids.results);

    Ok(())
}

#[test]
fn query_gt() -> Result<(), DocDbError> {
    let tmp_dir = tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    insert_test_data(&db)?;

    let ids = query::search_index(
        &db,
        vec![query::QP::GT {
            p: keypath!["name"],
            v: tv("john"),
        }],
    )?;
    assert_eq!(vec!["doc1".to_string()], ids.results);
    Ok(())
}

#[test]
fn query_lt() -> Result<(), DocDbError> {
    let tmp_dir = tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    insert_test_data(&db)?;

    let ids = query::search_index(
        &db,
        vec![query::QP::LT {
            p: keypath!["age"],
            v: tv(40),
        }],
    )?;
    assert_eq!(vec!["doc2".to_string()], ids.results);
    Ok(())
}

#[test]
fn query_lte() -> Result<(), DocDbError> {
    let tmp_dir = tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    insert_test_data(&db)?;

    let ids = query::search_index(
        &db,
        vec![query::QP::LTE {
            p: keypath!["age"],
            v: tv(40),
        }],
    )?;
    assert_eq!(vec!["doc1".to_string(), "doc2".to_string()], ids.results);
    Ok(())
}

#[test]
fn query_search_short_circuit_empty_scan() -> Result<(), DocDbError> {
    let tmp_dir = tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    insert_test_data(&db)?;

    let ids = query::search_index(
        &db,
        vec![
            query::QP::LTE {
                p: keypath!["age"],
                v: tv(4),
            },
            query::QP::E {
                p: keypath!["name"],
                v: tv("john"),
            },
        ],
    )?;
    assert_eq!(0, ids.results.len(), "wrong result count");
    assert_eq!(1, ids.stats.scans, "index scans not short circuited");
    let ids = query::search_index(
        &db,
        vec![
            query::QP::E {
                p: keypath!["name"],
                v: tv("john"),
            },
            query::QP::LTE {
                p: keypath!["age"],
                v: tv(4),
            },
        ],
    )?;
    assert_eq!(0, ids.results.len(), "wrong result count");
    // age is moved to the start as queries are internally ordered
    // by field name before being executed.
    assert_eq!(1, ids.stats.scans, "index scans not short circuited");
    Ok(())
}

#[test]
fn query_search_collapse_scans() -> Result<(), DocDbError> {
    let tmp_dir = tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    insert_test_data(&db)?;

    let ids = query::search_index(
        &db,
        vec![
            query::QP::LTE {
                p: keypath!["age"],
                v: tv(40),
            },
            query::QP::E {
                p: keypath!["age"],
                v: tv(40),
            },
            query::QP::E {
                p: keypath!["age"],
                v: tv(40),
            },
            query::QP::E {
                p: keypath!["age2"],
                v: tv(4),
            },
            query::QP::E {
                p: keypath!["age2"],
                v: tv(4),
            },
        ],
    )?;
    assert_eq!(0, ids.results.len(), "wrong result count");
    assert_eq!(2, ids.stats.scans, "index scans collapsed for same field");
    Ok(())
}
