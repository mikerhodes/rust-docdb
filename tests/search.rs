use rust_docdb::docdb;
use rust_docdb::keypath;
use rust_docdb::query;
use rust_docdb::query::tv;
use rust_docdb::query::TaggableValue;
use serde_json::json;
use sled;
use sled::Db;
use tempfile::tempdir;

fn insert_test_data(db: &Db) -> Result<(), sled::Error> {
    docdb::insert_document(
        &db,
        "doc1",
        json!({"a":{"b": 1}, "name": "mike", "age": 40, "pet": ["cat", "cat", "dog"]}),
    )?;
    docdb::insert_document(
        &db,
        "doc2",
        json!({"a":{"c": 2}, "name": "john", "age": 24}),
    )?;
    docdb::insert_document(
        &db,
        "doc3",
        json!({"a":{"c": 2}, "name": "john", "age": 110, "pet": ["wombat"]}),
    )?;
    Ok(())
}

#[test]
fn query_eq() -> Result<(), sled::Error> {
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
    assert_eq!(vec!["doc2".to_string(), "doc3".to_string()], ids);

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
    assert_eq!(vec!["doc3".to_string()], ids);

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
    assert_eq!(Vec::<String>::new(), ids);

    Ok(())
}

#[test]
fn query_array_eq() -> Result<(), sled::Error> {
    let tmp_dir = tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    insert_test_data(&db)?;

    docdb::insert_document(
        &db,
        "arrayed",
        json!({"arrs": [{"animals": ["cat", "wombat", "possum"]}, {"animals": "shark", "nums": [1,2,3,4,5]}]}),
    )?;
    docdb::insert_document(
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
    assert_eq!(vec!["arrayed".to_string()], ids);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 0, "animals", 2],
            v: tv("possum"),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string()], ids);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 1, "animals", 2],
            v: tv("possum"),
        }],
    )?;
    assert_eq!(Vec::<String>::new(), ids);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 0, "animals"],
            v: tv("shark"),
        }],
    )?;
    assert_eq!(vec!["arrayed2".to_string()], ids);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 1, "nums", 2],
            v: tv(3),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string(), "arrayed2".to_string()], ids);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 0, "nums", 2],
            v: tv(3),
        }],
    )?;
    assert_eq!(Vec::<String>::new(), ids);

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: keypath!["arrs", 0, "animals"], // check that we can change schema from array to string
            v: tv("shark"),
        }],
    )?;
    assert_eq!(vec!["arrayed2".to_string()], ids);

    Ok(())
}

#[test]
fn query_gte() -> Result<(), sled::Error> {
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
        ids
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
    assert_eq!(vec!["doc3".to_string()], ids);

    docdb::insert_document(&db, "arrayed", json!({"arr": [1,2,"foo",4]}))?;
    let ids = query::search_index(
        &db,
        vec![query::QP::GTE {
            p: keypath!["arr", 2],
            v: tv(-1),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string()], ids);
    let ids = query::search_index(
        &db,
        vec![query::QP::GTE {
            p: keypath!["arr", 2],
            v: tv(-1),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string()], ids);
    let ids = query::search_index(
        &db,
        vec![query::QP::GTE {
            p: keypath!["arr", 2],
            v: tv("bar"),
        }],
    )?;
    assert_eq!(vec!["arrayed".to_string()], ids);

    Ok(())
}
