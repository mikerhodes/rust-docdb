// end to end tests
use rust_docdb::{
    docdb, keypath,
    query::{search_index, tv, TaggableValue, QP},
};
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
        docdb::set_document(&db, &key, v).is_ok(),
        "doc should have been inserted"
    );
    assert!(
        docdb::get_document(&db, &key).is_ok_and(|o| o.is_some_and(|doc| {
            assert_eq!(doc.to_string(), expected);
            true
        }))
    );
}

#[test]
fn test_delete() -> Result<(), sled::Error> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db = docdb::new_database(tmp_dir.path()).unwrap();
    let v = json!({
        "name": "John Doe",
        "age": 43,
    });
    let docid = "foo".to_string();

    assert!(
        docdb::set_document(&db, &docid, v).is_ok(),
        "doc should have been inserted"
    );
    assert!(
        docdb::get_document(&db, &docid).is_ok_and(|x| x.is_some()),
        "document was not deleted"
    );
    assert!(
        docdb::delete_document(&db, &docid).is_ok(),
        "doc should have been deleted"
    );

    // Check we cannot get it by ID
    assert!(
        docdb::get_document(&db, &docid).is_ok_and(|x| x.is_none()),
        "document was not deleted"
    );
    // Or search for it
    assert!(
        search_index(
            &db,
            vec![QP::E {
                p: keypath!["name"],
                v: tv("John Doe"),
            }],
        )
        .is_ok_and(|result| result.len() == 0),
        "document id found via search"
    );
    assert!(
        search_index(
            &db,
            vec![QP::E {
                p: keypath!["age"],
                v: tv(43),
            }],
        )
        .is_ok_and(|result| result.len() == 0),
        "document id found via search"
    );

    Ok(())
}
