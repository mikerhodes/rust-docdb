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

    let mut path_values = vec![];
    get_path_values(&v, vec![], &mut path_values);

    for v in &path_values {
        println!("pathvalue: {:?}", v)
    }

    Ok(())
}

// get_path_values returns a Vector of (path, value) tuples. We use the json_serde::Value type
// so we carry around some type information for later encoding.
fn get_path_values<'a>(v: &'a Value, path: Vec<String>, acc: &mut Vec<(Vec<String>, &'a Value)>) {
    let mut stack = vec![(v, path)];

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
}
