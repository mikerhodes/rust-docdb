use sled::Db;

use crate::encoding::{self, TaggableValue};

#[allow(dead_code)] // I think this would go away if we had integration tests
pub fn lookup_eq(
    db: &Db,
    path: &Vec<String>,
    v: &TaggableValue,
) -> Result<Vec<String>, sled::Error> {
    let mut ids = vec![];
    let start_key = encoding::encode_index_query_start_key(path, v);
    let end_key = encoding::encode_index_query_end_key(path, v);

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
            &"doc1".to_string(),
            json!({"a":{"b": 1}, "name": "mike", "age": 40}),
        )?;
        docdb::insert_document(
            &db,
            &"doc2".to_string(),
            json!({"a":{"c": 2}, "name": "john", "age": 24}),
        )?;
        docdb::insert_document(
            &db,
            &"doc3".to_string(),
            json!({"a":{"c": 2}, "name": "john", "age": 110}),
        )?;

        let ids = lookup_eq(
            &db,
            &vec!["name".to_string()],
            &TaggableValue::String("john".to_string()),
        )?;
        assert_eq!(vec!["doc2".to_string(), "doc3".to_string()], ids);
        let ids = lookup_eq(
            &db,
            &vec!["a".to_string(), "b".to_string()],
            &TaggableValue::Number(1.0),
        )?;
        assert_eq!(vec!["doc1".to_string()], ids);
        let ids = lookup_eq(
            &db,
            &vec!["a".to_string(), "b".to_string()],
            &TaggableValue::Number(2.0),
        )?;
        assert_eq!(Vec::<String>::new(), ids);
        let ids = lookup_eq(
            &db,
            &vec!["a".to_string(), "c".to_string()],
            &TaggableValue::Number(2.0),
        )?;
        assert_eq!(vec!["doc2".to_string(), "doc3".to_string()], ids);

        Ok(())
    }
}
