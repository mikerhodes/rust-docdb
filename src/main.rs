use std::rc::Rc;

use serde_json::{json, Value};
use sled::Db;

use crate::encoding::encode_tagged_value;
use crate::encoding::TaggableValue;

mod encoding;

fn main() -> Result<(), u16> {
    let db = new_database(std::path::Path::new("docdb.data")).unwrap();

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
    let key = "foo".to_string();
    insert_document(&db, &key, v);
    get_document(&db, &key);
    db.remove(&key);

    Ok(())
}

// Retrieve a document from db by key.
fn get_document(db: &Db, key: &String) {
    let readvalue = db.get(key).unwrap();
    let frommsgpack = rmp_serde::from_slice::<Value>(&readvalue.unwrap()).unwrap();
    println!("{}", frommsgpack.to_string());
}

// Insert and index v into db at key
fn insert_document(db: &Db, docid: &String, v: serde_json::Value) {
    // pack the json into msgpack for storage
    let buf = rmp_serde::to_vec(&v).unwrap();
    db.insert(&docid, buf);

    // v is moved into get_path_values. This might not be possible
    // if we later needed v, but we don't yet.
    let path_values = get_path_values(v);

    let sentinal_value: [u8; 0] = [];
    // Here we would be indexing the path_values, so we can
    // consume them as we don't need them afterwards
    for (path, v) in path_values {
        // we will push everything into the key using
        // the tagged form. Paths must be tagged as they
        // can contain strings and array indexes (ints).
        // Tagging the value is obviously needed.
        // As we've tagged everything else, we may as
        // well tag the doc ID at the end too, so we
        // can uniformly decode using generic functions.
        println!(
            "pathvalue: {:?} => {:?}",
            path,
            encoding::encode_tagged_value(v.clone())
        );
        let mut pre_key = path;
        pre_key.push(v);
        pre_key.push(TaggableValue::String(docid.clone()));
        println!("pre_key: {:?}", pre_key);

        let key: Vec<Vec<u8>> = pre_key
            .into_iter()
            .map(|x| encode_tagged_value(x))
            .collect();

        // TODO we need the prefix keys for the docs and the index.
        let k = key.join(&0x00_u8);
        println!("k: {:?}", k);

        db.insert(&k, &sentinal_value);

        // key = encode the key
        // value = encode the value
        // insert into the database
    }
}

fn new_database(path: &std::path::Path) -> sled::Result<Db> {
    // return sled::open(path);
    // works like std::fs::open
    let db = sled::open(path)?;

    // key and value types can be `Vec<u8>`, `[u8]`, or `str`.
    let key = "my key";

    // `generate_id`
    let value = db.generate_id()?.to_be_bytes();

    dbg!(
        db.insert(key, &value)?, // as in BTreeMap::insert
        db.get(key)?,            // as in BTreeMap::get
        db.remove(key)?,         // as in BTreeMap::remove
    );

    Ok(db)
}

// get_path_values returns a Vector of (path, value) tuples. We use the json_serde::Value type
// so we carry around some type information for later encoding.
// v is moved into get_path_values and any needed Values end up moved into the function's return value
fn get_path_values(v: Value) -> Vec<(Vec<TaggableValue>, TaggableValue)> {
    let mut acc = vec![];
    let mut stack = vec![(vec![], v)];

    while let Some((path, v)) = stack.pop() {
        match v {
            Value::Array(a) => {
                for (i, v) in a.into_iter().enumerate() {
                    let mut p = path.clone();
                    p.push(TaggableValue::Number(i as f64));
                    stack.push((p, v))
                }
            }
            Value::Object(o) => {
                for (k, v) in o {
                    let mut p = path.clone();
                    p.push(TaggableValue::RcString(Rc::new(k)));
                    stack.push((p, v))
                }
            }
            Value::String(v) => acc.push((path, TaggableValue::String(v))),
            Value::Number(v) => {
                let fl = v.as_f64().unwrap();
                acc.push((path, TaggableValue::Number(fl)));
            }
            Value::Bool(v) => acc.push((path, TaggableValue::Bool(v))),
            Value::Null => acc.push((path, TaggableValue::Null)),
        }
    }

    acc
}
