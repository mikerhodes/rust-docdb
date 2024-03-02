use serde_json::Value;
use sled::Db;

use crate::encoding::{encode_document_key, encode_index_key};
use crate::pathvalues::get_path_values;

#[derive(Debug)]
pub enum DocDbError {
    GenericError,
    DocDecode(rmp_serde::decode::Error),
    DocEncode(rmp_serde::encode::Error),
    // A Db error indicates the underlying file
    // has become corrupted. The consuming application
    // should likely print out the error and then crash.
    Db(sled::Error),
}

impl From<sled::Error> for DocDbError {
    fn from(value: sled::Error) -> Self {
        DocDbError::Db(value)
    }
}

impl From<rmp_serde::encode::Error> for DocDbError {
    fn from(value: rmp_serde::encode::Error) -> Self {
        DocDbError::DocEncode(value)
    }
}

// Retrieve a document from db by key.
pub fn get_document(db: &Db, docid: &str) -> Result<Option<serde_json::Value>, DocDbError> {
    let readvalue = db.get(encode_document_key(docid))?;
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
pub fn set_document(db: &Db, docid: &str, v: serde_json::Value) -> Result<(), DocDbError> {
    let mut batch = sled::Batch::default();
    match get_document(&db, &docid)? {
        Some(v) => delete_batch(&mut batch, docid, v),
        None => {}
    };
    insert_batch(&mut batch, docid, v)?;
    db.apply_batch(batch).map_err(|e| DocDbError::Db(e))
}

// Adds commands to add and index `v` to the database to a batch
fn insert_batch(
    batch: &mut sled::Batch,
    docid: &str,
    v: serde_json::Value,
) -> Result<(), DocDbError> {
    // pack the json into msgpack for storage
    let buf = rmp_serde::to_vec(&v)?;
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

    Ok(())
}

pub fn delete_document(db: &Db, docid: &str) -> Result<(), DocDbError> {
    // If the document isn't in the database, assume it's okay
    let mut batch = sled::Batch::default();
    match get_document(&db, &docid)? {
        Some(v) => delete_batch(&mut batch, docid, v),
        None => {}
    };
    db.apply_batch(batch).map_err(|e| DocDbError::Db(e))
}

// Adds commands to remove v from the database to a batch
fn delete_batch(batch: &mut sled::Batch, docid: &str, v: serde_json::Value) {
    let path_values = get_path_values(v);
    for (path, v) in path_values {
        let k = encode_index_key(docid, &path, &v);
        batch.remove(k);
    }
    batch.remove(encode_document_key(docid));
}

pub fn new_database(path: &std::path::Path) -> sled::Result<Db> {
    // return sled::open(path);
    // works like std::fs::open
    let db = sled::open(path)?;
    Ok(db)
}
