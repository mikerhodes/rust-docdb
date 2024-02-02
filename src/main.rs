use serde_json::{json, Value};
use sled::Db;

fn main() -> serde_json::Result<()> {
    let _ = new_database(std::path::Path::new("docdb.data"));
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

    // v is moved into get_path_values. This might not be possible
    // if we later needed v, but we don't yet.
    let path_values = get_path_values(v);

    // Here we would be indexing the path_values, so we can
    // consume them as we don't need them afterwards
    for (path, v) in path_values {
        println!("pathvalue: {:?} => {}", path, v)
    }

    Ok(())
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
    String(String),
    ArrayIndex(usize),
}
// get_path_values returns a Vector of (path, value) tuples. We use the json_serde::Value type
// so we carry around some type information for later encoding.
// v is moved into get_path_values and any needed Values end up moved into the function's return value
fn get_path_values(v: Value) -> Vec<(Vec<PathComponent>, Value)> {
    let mut acc = vec![];
    let mut stack = vec![(vec![], v)];

    while stack.len() > 0 {
        let (path, v) = stack.pop().unwrap();
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
                    p.push(PathComponent::String(k));
                    stack.push((p, v))
                }
            }
            _ => acc.push((path, v)),
        }
    }

    acc
}
