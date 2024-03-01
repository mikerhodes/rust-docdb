use serde_json::Value;
use sled::Db;

use crate::encoding::{encode_document_key, encode_index_key};
use crate::pathvalues::get_path_values;

#[derive(Debug)]
pub enum DocDbError {
    DocDecode(rmp_serde::decode::Error),
    Db(sled::Error),
}

// Retrieve a document from db by key.
pub fn get_document(db: &Db, docid: &str) -> Result<Option<serde_json::Value>, DocDbError> {
    let readvalue = match db.get(encode_document_key(docid)) {
        Ok(s) => s,
        Err(e) => return Err(DocDbError::Db(e)),
    };
    let packed = match readvalue {
        Some(doc) => doc,
        None => return Ok(None),
    };
    let doc = match rmp_serde::from_slice::<Value>(&packed) {
        Ok(d) => d,
        Err(e) => return Err(DocDbError::DocDecode(e)),
    };
    Ok(Some(doc))
}

// Insert and index v into db at key
pub fn insert_document(db: &Db, docid: &str, v: serde_json::Value) -> Result<(), sled::Error> {
    let mut batch = sled::Batch::default();

    // pack the json into msgpack for storage
    let buf = rmp_serde::to_vec(&v).unwrap();
    batch.insert(encode_document_key(docid), buf);

    // v is moved into get_path_values. This might not be possible
    // if we later needed v, but we don't yet.
    let path_values = get_path_values(v);

    let sentinal_value: [u8; 0] = [];
    // Here we would be indexing the path_values, so we can
    // consume them as we don't need them afterwards
    for (path, v) in path_values {
        let k = encode_index_key(docid, &path, &v);
        batch.insert(k, &sentinal_value);
    }

    db.apply_batch(batch)
}

pub fn delete_document(db: &Db, docid: &str) -> Result<(), DocDbError> {
    // If the document isn't in the database, assume it's okay
    let v = match get_document(&db, &docid)? {
        Some(v) => v,
        None => return Ok(()),
    };

    let mut batch = sled::Batch::default();
    let path_values = get_path_values(v);
    for (path, v) in path_values {
        let k = encode_index_key(docid, &path, &v);
        batch.remove(k);
    }
    batch.remove(encode_document_key(docid));

    return match db.apply_batch(batch) {
        Ok(_) => Ok(()),
        Err(e) => Err(DocDbError::Db(e)),
    };
}

pub fn new_database(path: &std::path::Path) -> sled::Result<Db> {
    // return sled::open(path);
    // works like std::fs::open
    let db = sled::open(path)?;
    Ok(db)
}
