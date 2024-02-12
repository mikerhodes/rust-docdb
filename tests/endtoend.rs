// end to end tests
use rust_docdb::docdb;
use serde_json::json;

#[test]
fn insert_and_readback() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();

    // The type of `john` is `serde_json::Value`
    let v = json!({
        "name": "John Doe",
        "age": 43,
        "phones": [
            "+44 1234567",
            "+44 2345678"
        ],
        "pets": {
            "frankie": {"species": "cat", "age": 3},
            "bennie": {"species": "cat", "age": 9},
        }
    });
    let expected = v.to_string();
    let key = "foo".to_string();

    assert!(
        docdb::insert_document(&db, &key, v).is_ok(),
        "doc should have been inserted"
    );
    match docdb::get_document(&db, &key) {
        Ok(n) => {
            assert_eq!(n.to_string(), expected, "document was not as expected")
        }
        Err(_) => assert!(false, "could not get document"),
    };
    assert!(db.remove(&key).is_ok(), "doc should have been deleted");
}
