use crate::encoding::TaggableValue;
use serde_json::Value;
use std::rc::Rc;

// get_path_values returns a Vector of (path, value) tuples. We use the json_serde::Value type
// so we carry around some type information for later encoding.
// v is moved into get_path_values and any needed Values end up moved into the function's return value
pub fn get_path_values(v: Value) -> Vec<(Vec<TaggableValue>, TaggableValue)> {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_get_path_value() {
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
        let path_values = get_path_values(v);
        let expected = vec![
            (
                vec![
                    TaggableValue::RcString(Rc::new("phones".to_string())),
                    TaggableValue::Number(1.0),
                ],
                TaggableValue::String("+44 2345678".to_string()),
            ),
            (
                vec![
                    TaggableValue::RcString(Rc::new("phones".to_string())),
                    TaggableValue::Number(0.0),
                ],
                TaggableValue::String("+44 1234567".to_string()),
            ),
            (
                vec![
                    TaggableValue::RcString(Rc::new("pets".to_string())),
                    TaggableValue::RcString(Rc::new("frankie".to_string())),
                    TaggableValue::RcString(Rc::new("species".to_string())),
                ],
                TaggableValue::String("cat".to_string()),
            ),
            (
                vec![
                    TaggableValue::RcString(Rc::new("pets".to_string())),
                    TaggableValue::RcString(Rc::new("frankie".to_string())),
                    TaggableValue::RcString(Rc::new("age".to_string())),
                ],
                TaggableValue::Number(3.0),
            ),
            (
                vec![
                    TaggableValue::RcString(Rc::new("pets".to_string())),
                    TaggableValue::RcString(Rc::new("bennie".to_string())),
                    TaggableValue::RcString(Rc::new("species".to_string())),
                ],
                TaggableValue::String("cat".to_string()),
            ),
            (
                vec![
                    TaggableValue::RcString(Rc::new("pets".to_string())),
                    TaggableValue::RcString(Rc::new("bennie".to_string())),
                    TaggableValue::RcString(Rc::new("age".to_string())),
                ],
                TaggableValue::Number(9.0),
            ),
            (
                vec![TaggableValue::RcString(Rc::new("name".to_string()))],
                TaggableValue::String("John Doe".to_string()),
            ),
            (
                vec![TaggableValue::RcString(Rc::new("age".to_string()))],
                TaggableValue::Number(43.0),
            ),
        ];

        assert_eq!(path_values, expected);
    }
}
