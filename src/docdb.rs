use serde_json::Value;
use sled::transaction::{abort, TransactionError};
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
pub fn insert_document(db: &Db, docid: &str, v: serde_json::Value) -> Result<(), DocDbError> {
    let data = match rmp_serde::to_vec(&v) {
        Ok(b) => b,
        Err(e) => return Err(DocDbError::DocEncode(e)),
    };
    let buf = &data[..];
    // TODO can we avoid the v clone() call?
    // The issue is that the closure in db.transaction can be called
    // multiple times, so the closure can't have things moved into it
    // as they'd then not be available to be moved if it is run again.
    let res: Result<(), TransactionError<()>> = db.transaction(|db_tx| {
        // pack the json into msgpack for storage
        db_tx.insert(encode_document_key(docid), buf)?;

        // v is moved into get_path_values. This might not be possible
        // if we later needed v, but we don't yet.
        let path_values = get_path_values(v.clone());

        let sentinal_value: [u8; 0] = [];
        // Here we are indexing the path_values, so we can
        // consume them as we don't need them afterwards
        for (path, v) in path_values {
            let k = encode_index_key(docid, &path, &v);
            db_tx.insert(k, &sentinal_value)?;
        }
        Ok(())
    });
    match res {
        Ok(_) => Ok(()),
        Err(e) => match e {
            TransactionError::Abort(_) => Err(DocDbError::GenericError),
            TransactionError::Storage(e) => Err(DocDbError::Db(e)),
        },
    }
}

pub fn delete_document(db: &Db, docid: &str) -> Result<(), DocDbError> {
    // 1. Read the existing value
    // 2. Generate the existing index entries from that, and delete them.
    // 3. Delete the document itself.
    let res: Result<(), TransactionError<DocDbError>> = db.transaction(|db_tx| {
        let readvalue = db_tx.get(encode_document_key(docid))?;
        // If the document isn't in the database, assume it's okay
        let packed = match readvalue {
            Some(doc) => doc,
            None => return Ok(()),
        };
        let v = match rmp_serde::from_slice::<Value>(&packed) {
            Ok(d) => d,
            Err(e) => abort(DocDbError::DocDecode(e))?,
        };

        let path_values = get_path_values(v.clone());
        for (path, v) in path_values {
            let k = encode_index_key(docid, &path, &v);
            db_tx.remove(k)?;
        }
        db_tx.remove(encode_document_key(docid))?;
        Ok(())
    });
    match res {
        Ok(_) => Ok(()),
        Err(e) => match e {
            TransactionError::Abort(_) => Err(DocDbError::GenericError),
            TransactionError::Storage(e) => Err(DocDbError::Db(e)),
        },
    }
}

pub fn new_database(path: &std::path::Path) -> sled::Result<Db> {
    // return sled::open(path);
    // works like std::fs::open
    let db = sled::open(path)?;
    Ok(db)
}
