use serde_json::{json, Result, Value};

fn main() -> Result<()> {
    // The type of `john` is `serde_json::Value`
    let v = json!({
        "name": "John Doe",
        "age": 43,
        "phones": [
            "+44 1234567",
            "+44 2345678"
        ]
    });

    get_path_values(&v);

    Ok(())
}

fn get_path_values(v: &Value) {
    match v {
        Value::Array(a) => println!("found an array {:?}", a),
        Value::Bool(b) => println!("found a bool {}", b),
        Value::Null => println!("found a null"),
        Value::Number(n) => println!("found a number {}", n),
        Value::Object(o) => println!("Found an object {:?}", o),
        Value::String(s) => println!("found a string {}", s),
    }
    // Access parts of the data by indexing with square brackets.
    println!("Please call {} at the number {}", v["name"], v["phones"][0]);
}
