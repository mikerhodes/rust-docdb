use std::rc::Rc;

pub fn encode_index_key(docid: &String, path: Vec<TaggableValue>, v: TaggableValue) -> Vec<u8> {
    // we will push everything into the key using
    // the tagged form. Paths must be tagged as they
    // can contain strings and array indexes (ints).
    // Tagging the value is obviously needed.
    // As we've tagged everything else, we may as
    // well tag the doc ID at the end too, so we
    // can uniformly decode using generic functions.
    // println!("pathvalue: {} {:?} => {:?}", docid, path, v,);
    let mut pre_key = path;
    pre_key.push(v);
    pre_key.push(TaggableValue::String(docid.clone()));
    // println!("pre_key: {:?}", pre_key);

    let key: Vec<Vec<u8>> = pre_key
        .into_iter()
        .map(|x| encode_tagged_value(x))
        .collect();

    // TODO we need the prefix keys for the docs and the index.
    // We could make those TaggableValue::KeyPrefix(u8) which
    // just returns the u8?
    let k = key.join(&0x00_u8);
    // println!("k: {:?}", k);
    k
}

enum JsonTag {
    // printable character makes easier debugging
    Null = 0x28,   // char: (
    False = 0x29,  // char: )
    True = 0x2a,   // char: *
    Number = 0x2b, // char: +
    String = 0x2c, // char: ,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TaggableValue {
    Null,
    Bool(bool),
    String(String),
    RcString(Rc<String>), // Rc<String> avoids cloning field name string buffers
    // ArrayIndex(usize), // Can we encode a usize more easily?
    Number(f64),
}

// encode_tagged_value encodes a primitive JSON type:
// number, string, null and bool.
// TODO return a Result<Vec<u8>>? So we can return an error
//      if it's not the right type. Perhaps the type system
//      can enforce it.
pub fn encode_tagged_value(v: TaggableValue) -> Vec<u8> {
    let mut tv = vec![];

    match v {
        TaggableValue::Null => tv.push(JsonTag::Null as u8),
        TaggableValue::Bool(b) => match b {
            true => tv.push(JsonTag::True as u8),
            false => tv.push(JsonTag::False as u8),
        },
        TaggableValue::Number(n) => {
            // This StackOverflow answer shows how to
            // encode a float64 into a byte array that
            // has the same sort order as the floats.
            // https://stackoverflow.com/a/54557561
            let fl = n;
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
        TaggableValue::String(s) => {
            tv.push(JsonTag::String as u8);
            tv.extend(s.into_bytes())
        }
        TaggableValue::RcString(s) => {
            tv.push(JsonTag::String as u8);
            tv.extend((*s).clone().into_bytes())
        }
    }

    tv
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    // Here we have only the simplest of tests. We reserve more complex
    // ones for when we are testing the full generation of keys.

    #[test]
    fn test_encode_null() {
        assert_eq!(
            encode_tagged_value(TaggableValue::Null),
            vec![JsonTag::Null as u8]
        );
    }

    #[test]
    fn test_encode_bool() {
        assert_eq!(
            encode_tagged_value(TaggableValue::Bool(true)),
            vec![JsonTag::True as u8]
        );
        assert_eq!(
            encode_tagged_value(TaggableValue::Bool(false)),
            vec![JsonTag::False as u8]
        );
    }

    #[test]
    fn test_encode_number() {
        assert_eq!(
            encode_tagged_value(TaggableValue::Number(-1_f64)),
            vec![
                0x2b, // JsonTag::Number
                0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff // -1
            ]
        );
    }

    #[test]
    fn test_encode_number_ordering() {
        let tests = vec![(1, 2), (-1, 1), (123, 321), (0, 1), (-1, 0)];
        for t in tests {
            assert!(
                encode_tagged_value(TaggableValue::Number(t.0 as f64))
                    < encode_tagged_value(TaggableValue::Number(t.1 as f64)),
            );
        }
    }

    #[test]
    fn test_encode_string() {
        assert_eq!(
            encode_tagged_value(TaggableValue::String("foo".to_string())),
            vec![
                0x2c, // JsonTag::String
                0x66, 0x6f, 0x6f, // foo
            ]
        );
    }

    #[test]
    fn test_encode_key() {
        assert_eq!(
            encode_index_key(
                &"foo".to_string(),
                vec![
                    TaggableValue::RcString(Rc::new("phones".to_string())),
                    TaggableValue::Number(1.0)
                ],
                TaggableValue::String("+44 2345678".to_string())
            ),
            vec![
                44, // JsonTag::String
                112, 104, 111, 110, 101, 115, // phones
                0,   // separator
                43,  //JsonTag::Number
                191, 240, 0, 0, 0, 0, 0, 0,  // 1.0
                0,  // sep
                44, //String
                43, 52, 52, 32, 50, 51, 52, 53, 54, 55, 56, // phone no
                0,  // sep
                44, // String
                102, 111, 111 // foo
            ]
        )
    }

    #[test]
    fn test_encode_key2() {
        assert_eq!(
            encode_index_key(
                &"foo".to_string(),
                vec![
                    TaggableValue::RcString(Rc::new("pets".to_string())),
                    TaggableValue::RcString(Rc::new("bennie".to_string())),
                    TaggableValue::RcString(Rc::new("age".to_string())),
                ],
                TaggableValue::Number(9.0),
            ),
            vec![
                44, //String
                112, 101, 116, 115, // pets
                0,   // sep
                44,  // String
                98, 101, 110, 110, 105, 101, // bennie
                0,   // sep
                44,  // String
                97, 103, 101, // age
                0,   // sep
                43,  // Number
                192, 34, 0, 0, 0, 0, 0, 0,  // 9
                0,  // sep
                44, // String
                102, 111, 111 // foo
            ]
        )
    }
}
