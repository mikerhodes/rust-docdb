use serde_json::{json, Result, Value};

fn main() -> Result<()> {
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

// get_path_values returns a Vector of (path, value) tuples. We use the json_serde::Value type
// so we carry around some type information for later encoding.
// v is moved into get_path_values and any needed Values end up moved into the function's return value
// For array support, we would put the array index into the path. Therefore we'd need to create an
// enum that could hold either String or int and it would be the type in the path vector returned.
fn get_path_values(v: Value) -> Vec<(Vec<String>, Value)> {
    let mut acc = vec![];
    let mut stack = vec![(v, vec![])];

    while stack.len() > 0 {
        let (v, path) = stack.pop().unwrap();
        match v {
            Value::Array(a) => println!("found an array (that we don't support yet) {:?}", a),
            Value::Bool(_) => acc.push((path, v)),
            Value::Null => acc.push((path, v)),
            Value::Number(_) => acc.push((path, v)),
            Value::Object(o) => {
                for (k, v) in o {
                    let mut p = path.clone();
                    p.push(k.clone());
                    stack.push((v, p))
                }
            }
            Value::String(_) => acc.push((path, v)),
        }
    }

    acc
}
