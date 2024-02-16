use serde_json::Value;
use sled::Db;

use crate::encoding::{encode_document_key, encode_index_key};
use crate::pathvalues::get_path_values;

// Retrieve a document from db by key.
pub fn get_document(db: &Db, docid: &String) -> Result<String, sled::Error> {
    let readvalue = match db.get(encode_document_key(docid)) {
        Ok(s) => s,
        Err(e) => return Err(e),
    };
    let frommsgpack = rmp_serde::from_slice::<Value>(&readvalue.unwrap()).unwrap();
    let result = frommsgpack.to_string();
    Ok(result)
}

// Insert and index v into db at key
pub fn insert_document(db: &Db, docid: &String, v: serde_json::Value) -> Result<(), sled::Error> {
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

pub fn new_database(path: &std::path::Path) -> sled::Result<Db> {
    // return sled::open(path);
    // works like std::fs::open
    let db = sled::open(path)?;
    Ok(db)
}
