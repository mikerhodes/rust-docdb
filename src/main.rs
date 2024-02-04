use serde_json::{json, Value};
use sled::Db;

fn main() -> serde_json::Result<()> {
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

    // pack the json into msgpack for storage -- try round-tripping it
    let buf = rmp_serde::to_vec(&v).unwrap();
    db.insert("foo", buf);
    let readvalue = db.get("foo").unwrap();
    db.remove("foo");
    let frommsgpack = rmp_serde::from_slice::<Value>(&readvalue.unwrap()).unwrap();
    println!("{:?}", frommsgpack.to_string());

    // v is moved into get_path_values. This might not be possible
    // if we later needed v, but we don't yet.
    let path_values = get_path_values(v);

    // Here we would be indexing the path_values, so we can
    // consume them as we don't need them afterwards
    for (path, v) in path_values {
        println!("pathvalue: {:?} => {}", path, v);
        println!("pathvalue: {:?} => {:?}", path, encode_tagged_value(v));
        // key = encode the key
        // value = encode the value
        // insert into the database
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

enum JsonTag {
    // printable character makes easier debugging
    Null = 0x28,   // char: (
    False = 0x29,  // char: )
    True = 0x2a,   // char: *
    Number = 0x2b, // char: +
    String = 0x2c, // char: ,
}

// encode_tagged_value encodes a primitive JSON type:
// number, string, null and bool.
// TODO copy the unit tests from the Go version
// TODO return a Result<Vec<u8>>? So we can return an error
//      if it's not the right type. Perhaps the type system
//      can enforce it.
fn encode_tagged_value(v: Value) -> Vec<u8> {
    let mut tv = vec![];

    match v {
        Value::Null => tv.push(JsonTag::Null as u8),
        Value::Bool(b) => match b {
            true => tv.push(JsonTag::True as u8),
            false => tv.push(JsonTag::False as u8),
        },
        Value::Number(n) => {
            // This StackOverflow answer shows how to
            // encode a float64 into a byte array that
            // has the same sort order as the floats.
            // https://stackoverflow.com/a/54557561
            let fl = n.as_f64().unwrap();
            let mut bits = fl.to_bits(); // creates a u64
            if fl >= 0_f64 {
                bits ^= 0x8000000000000000
            } else {
                bits ^= 0xffffffffffffffff
            }
            let buf = bits.to_be_bytes();

            tv.push(JsonTag::Number as u8);
            tv.extend_from_slice(&buf)
        }
        Value::String(s) => {
            tv.push(JsonTag::String as u8);
            tv.extend(s.into_bytes())
        }
        _ => {
            println!("ERROR found object or array in encode_tagged_value!")
        }
    }

    tv
}
