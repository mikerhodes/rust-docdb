use std::error::Error;
use std::{fmt, str};

use crate::query::TaggableValue;

// An error for decoding keys
#[derive(Debug, Clone)]
pub struct DecodeError;
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error decoding key")
    }
}
impl Error for DecodeError {}

// These consts are used at the start of keys to differentiate
// keys for primary document data from index data. They are
// prefixed to the encoded keys.
const KEY_DOCUMENT: u8 = 1u8;
const KEY_INDEX: u8 = 2u8;

pub fn encode_document_key(docid: &str) -> Vec<u8> {
    let mut k: Vec<u8> = vec![KEY_DOCUMENT, 0x00];
    k.extend(docid.as_bytes());
    k
}
pub fn encode_index_key(docid: &str, path: &Vec<TaggableValue>, v: &TaggableValue) -> Vec<u8> {
    // we will push everything into the key using
    // the tagged form. Paths must be tagged as they
    // can contain strings and array indexes (ints).
    // Tagging the value is obviously needed.
    // As we've tagged everything else, we may as
    // well tag the doc ID at the end too, so we
    // can uniformly decode using generic functions.
    let mut k: Vec<u8> = vec![KEY_INDEX, 0x00];
    for component in path {
        k.extend(encode_tagged_value(component));
        k.push(0x00);
    }
    k.extend(encode_tagged_value(v));
    k.push(0x00);
    k.extend(encode_tagged_value(&TaggableValue::from(docid)));
    k
}

// Decodes the doc ID from index key k
pub fn decode_index_key_docid(k: &[u8]) -> Result<&str, DecodeError> {
    let last = k.split(|b| *b == 0x00).last();
    match last {
        Some(v) => decode_tagged_str(v),
        None => Err(DecodeError),
    }
}

// Decodes a tagged value into a &str
fn decode_tagged_str(tv: &[u8]) -> Result<&str, DecodeError> {
    let (tag, tail) = tv.split_first().ok_or(DecodeError)?;
    match *tag {
        x if x == JsonTag::String as u8 => match str::from_utf8(tail) {
            Ok(v) => Ok(v),
            Err(_) => Err(DecodeError),
        },
        _ => Err(DecodeError),
    }
}

// Encode an index key that is guaranteed to be the first key of indexed path and v.
pub fn encode_index_query_start_key(path: &Vec<&str>, v: &TaggableValue) -> Vec<u8> {
    let mut k: Vec<u8> = vec![KEY_INDEX, 0x00];
    for component in path {
        let tv = TaggableValue::from(*component);
        k.extend(encode_tagged_value(&tv));
        k.push(0x00);
    }
    k.extend(encode_tagged_value(v));
    k.push(0x00);
    k
}

// Encode an index key that is guaranteed to be after all values with given path and v, but
// before any different path and v.
pub fn encode_index_query_end_key(path: &Vec<&str>, v: &TaggableValue) -> Vec<u8> {
    let mut k: Vec<u8> = vec![KEY_INDEX, 0x00];
    for component in path {
        let tv = TaggableValue::from(*component);
        k.extend(encode_tagged_value(&tv));
        k.push(0x00);
    }
    k.extend(encode_tagged_value(v));
    k.push(0x01); // ie, 0x01 is always greater than the sep between path/value and doc ID
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

// encode_tagged_value encodes a primitive JSON type:
// number, string, null and bool.
// TODO return a Result<Vec<u8>>? So we can return an error
//      if it's not the right type. Perhaps the type system
//      can enforce it.
pub fn encode_tagged_value(v: &TaggableValue) -> Vec<u8> {
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
            let fl = *n;
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
            tv.extend(s.as_bytes())
        }
        TaggableValue::RcString(s) => {
            tv.push(JsonTag::String as u8);
            tv.extend(s.as_bytes())
        }
    }

    tv
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use std::rc::Rc;

    #[test]
    fn test_encode_document_key() {
        let tests = vec![
            ("foo".to_string(), vec![0x66, 0x6f, 0x6f]),
            ("møkå".to_string(), vec![0x6d, 0xc3, 0xb8, 0x6b, 0xc3, 0xa5]),
        ];
        for t in tests {
            let mut expected = vec![KEY_DOCUMENT, 0x00];
            expected.extend(t.1);
            assert_eq!(encode_document_key(&t.0), expected);
        }
    }

    #[test]
    fn test_encode_null() {
        assert_eq!(
            encode_tagged_value(&TaggableValue::Null),
            vec![JsonTag::Null as u8]
        );
    }

    #[test]
    fn test_encode_bool() {
        assert_eq!(
            encode_tagged_value(&TaggableValue::Bool(true)),
            vec![JsonTag::True as u8]
        );
        assert_eq!(
            encode_tagged_value(&TaggableValue::Bool(false)),
            vec![JsonTag::False as u8]
        );
    }

    #[test]
    fn test_encode_number() {
        assert_eq!(
            encode_tagged_value(&TaggableValue::from(-1)),
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
                encode_tagged_value(&TaggableValue::from(t.0))
                    < encode_tagged_value(&TaggableValue::from(t.1)),
            );
        }
    }

    #[test]
    fn test_encode_string() {
        assert_eq!(
            encode_tagged_value(&TaggableValue::from("foo")),
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
                &vec![
                    TaggableValue::RcString(Rc::new("phones".to_string())),
                    TaggableValue::from(1)
                ],
                &TaggableValue::from("+44 2345678")
            ),
            vec![
                2,  // KEY_INDEX
                0,  // separator
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
                &vec![
                    TaggableValue::RcString(Rc::new("pets".to_string())),
                    TaggableValue::RcString(Rc::new("bennie".to_string())),
                    TaggableValue::RcString(Rc::new("age".to_string())),
                ],
                &TaggableValue::from(9),
            ),
            vec![
                2,  // KEY_INDEX
                0,  // separator
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
