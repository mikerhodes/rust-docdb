use std::rc::Rc;

use serde_json::{json, Value};
use sled::Db;

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
fn insert_document(db: &Db, key: &String, v: serde_json::Value) {
    // pack the json into msgpack for storage
    let buf = rmp_serde::to_vec(&v).unwrap();
    db.insert(&key, buf);

    // v is moved into get_path_values. This might not be possible
    // if we later needed v, but we don't yet.
    let path_values = get_path_values(v);

    // Here we would be indexing the path_values, so we can
    // consume them as we don't need them afterwards
    for (path, v) in path_values {
        println!(
            "pathvalue: {:?} => {:?}",
            path,
            encoding::encode_tagged_value(v)
        );
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

#[derive(Debug, Clone)]
enum PathComponent {
    FieldName(Rc<String>), // Rc<String> avoids cloning field name string buffers many times
    ArrayIndex(usize),
}
// get_path_values returns a Vector of (path, value) tuples. We use the json_serde::Value type
// so we carry around some type information for later encoding.
// v is moved into get_path_values and any needed Values end up moved into the function's return value
fn get_path_values(v: Value) -> Vec<(Vec<PathComponent>, Value)> {
    let mut acc = vec![];
    let mut stack = vec![(vec![], v)];

    while let Some((path, v)) = stack.pop() {
        match v {
            Value::Array(a) => {
                for (i, v) in a.into_iter().enumerate() {
                    let mut p = path.clone();
                    p.push(PathComponent::ArrayIndex(i));
                    stack.push((p, v))
                }
            }
            Value::Object(o) => {
                for (k, v) in o {
                    let mut p = path.clone();
                    p.push(PathComponent::FieldName(Rc::new(k)));
                    stack.push((p, v))
                }
            }
            _ => acc.push((path, v)),
        }
    }

    acc
}
