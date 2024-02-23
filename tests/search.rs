use rust_docdb::docdb;
use rust_docdb::query;
use rust_docdb::query::TaggableValue;
use serde_json::json;
use sled;
use tempfile::tempdir;

#[test]
fn searching() -> Result<(), sled::Error> {
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

    let ids = query::search_index(
        &db,
        vec![query::QP::E {
            p: vec!["name"],
            v: TaggableValue::from("john"),
        }],
    )?;
    assert_eq!(vec!["doc2".to_string(), "doc3".to_string()], ids);

    Ok(())
}
