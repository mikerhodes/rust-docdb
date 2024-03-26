use std::error::Error;
use std::{fmt, str};

use crate::query::TaggableValue;

// cribbed from https://stackoverflow.com/a/75994861 --- this
// allows us to create a vec of TaggableValues from normal
// variables and literals.
// #[macro_export]
// macro_rules! keypath {
//     () => { vec![] };
//     ($elem:expr; $n:expr) => { vec![TaggableValue::from($elem); $n] };
//     ($($x:expr),+ $(,)?) => { vec![$(TaggableValue::from($x)),+] };
// }

// Let's use this one. I can actually understand it, learner that I am.
#[macro_export]
macro_rules! keypath {
    ( $( $x:expr ),* ) => {
        {
            let mut temp_vec = Vec::<TaggableValue>::new();
            $(
                temp_vec.push(TaggableValue::from($x));
            )*
            temp_vec
        }
    };
}

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
pub(crate) const KEY_INDEX: u8 = 2u8;

// Use a separate SEP for between path components to make
// it easier to split the path from the other key components.
const COMPONENT_SEP: u8 = 0x00;
const PATH_SEP: u8 = 0x01;

pub(crate) trait IndexKey {
    fn path_prefix(&self) -> Option<&[u8]>;
}

impl IndexKey for Vec<u8> {
    // path_prefix will be sliced from key
    fn path_prefix(&self) -> Option<&[u8]> {
        // key format idx SEP path SEP value SEP docid
        //             slice here ^
        assert!(self[0] == KEY_INDEX);
        let rest = &self[2..];
        let path_len = path_length(rest);
        Some(&rest[..path_len])
    }
}

// path_length returns the number of bytes that the path
// starting at s[0] takes up.
// This allows one to return a slice from an index key
// containing only the path.
fn path_length(s: &[u8]) -> usize {
    // Path is either string or number
    let mut idx = 0;
    while idx < s.len() {
        println!("byte: {}", s[idx]);
        // Each iteration decodes on field of the path
        if s[idx] == JsonTag::String as u8 {
            while idx < s.len() {
                if s[idx] == PATH_SEP || s[idx] == COMPONENT_SEP {
                    break;
                }
                idx += 1
            }
        } else if s[idx] == JsonTag::Number as u8 {
            idx += 9
        } else {
            assert!(false, "unexpected start of path part");
        }
        if s[idx] == COMPONENT_SEP {
            break;
        }
        assert!(s[idx] == PATH_SEP, "expected path sep");
        idx += 1;
    }
    idx
}

pub fn encode_document_key(docid: &str) -> Vec<u8> {
    let mut k: Vec<u8> = vec![KEY_DOCUMENT, 0x00];
    k.extend(&TaggableValue::from(docid).encode());
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
    k.extend(path.encode());
    k.push(0x00);
    k.extend(v.encode());
    k.push(0x00);
    k.extend(&TaggableValue::from(docid).encode());
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
        x if x == JsonTag::String as u8 => str::from_utf8(tail).map_err(|_| DecodeError),
        // Only support decoding strings
        _ => Err(DecodeError),
    }
}

// Encode an index key that is guaranteed to be the lower
// bound of keys with a given path.
pub fn encode_index_query_p_start_key(path: &Vec<TaggableValue>) -> Vec<u8> {
    query_lower_bound(path, None)
}

// Encode an index key that is guaranteed to be the first key of indexed path and v.
pub fn encode_index_query_pv_start_key(path: &Vec<TaggableValue>, v: &TaggableValue) -> Vec<u8> {
    query_lower_bound(path, Some(v))
}

pub fn query_lower_bound(p: &Vec<TaggableValue>, v: Option<&TaggableValue>) -> Vec<u8> {
    let mut k: Vec<u8> = vec![KEY_INDEX, 0x00];
    k.extend(p.encode());
    v.inspect(|v| {
        k.push(0x00);
        k.extend(v.encode());
    });
    k.push(0x00);
    k
}

// Encode an index key that is guaranteed to be after all values with
// given path, but
// before any different path and v.
pub fn encode_index_query_p_end_key(path: &Vec<TaggableValue>) -> Vec<u8> {
    query_upper_bound(path, None)
}
// Encode an index key that is guaranteed to be after all values with given path and v, but
// before any different path and v.
pub fn encode_index_query_pv_end_key(path: &Vec<TaggableValue>, v: &TaggableValue) -> Vec<u8> {
    query_upper_bound(path, Some(v))
}
pub fn query_upper_bound(p: &Vec<TaggableValue>, v: Option<&TaggableValue>) -> Vec<u8> {
    let mut k: Vec<u8> = vec![KEY_INDEX, 0x00];
    k.extend(p.encode());
    v.inspect(|v| {
        k.push(0x00);
        k.extend(v.encode());
    });
    k.push(0x02); // This must be greater than both component and path sep
    k
}

// Encodable is a small private trait that helps us encode
// each type of value we use in our keys
pub(crate) trait Encodable {
    fn encode(&self) -> Vec<u8>;
}

impl Encodable for &Vec<TaggableValue> {
    // encode path, separated with 0x00 and excluding trailing 0x00
    fn encode(&self) -> Vec<u8> {
        let mut k: Vec<u8> = vec![];
        if let Some((last, elements)) = self.split_last() {
            for component in elements {
                k.extend(component.encode());
                k.push(PATH_SEP);
            }
            k.extend(last.encode());
        }
        k
    }
}

enum JsonTag {
    // printable character makes easier debugging
    Null = 0x28,   // char: (
    False = 0x29,  // char: )
    True = 0x2a,   // char: *
    Number = 0x2b, // char: +
    String = 0x2c, // char: ,
}

impl Encodable for TaggableValue {
    // encode_tagged_value encodes a primitive JSON type:
    // number, string, null and bool.
    fn encode(&self) -> Vec<u8> {
        let mut tv = vec![];

        match self {
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
}

#[cfg(test)]
mod tests {
    use crate::query::tv;

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use std::rc::Rc;

    #[test]
    fn test_encode_document_key() {
        let tests = vec![
            ("foo".to_string(), vec![44, 0x66, 0x6f, 0x6f]),
            (
                "møkå".to_string(),
                vec![44, 0x6d, 0xc3, 0xb8, 0x6b, 0xc3, 0xa5],
            ),
        ];
        for t in tests {
            let mut expected = vec![KEY_DOCUMENT, 0x00];
            expected.extend(t.1);
            assert_eq!(encode_document_key(&t.0), expected);
        }
    }

    #[test]
    fn test_encode_null() {
        assert_eq!(TaggableValue::Null.encode(), vec![JsonTag::Null as u8]);
    }

    #[test]
    fn test_encode_bool() {
        assert_eq!(tv(true).encode(), vec![JsonTag::True as u8]);
        assert_eq!(tv(false).encode(), vec![JsonTag::False as u8]);
    }

    #[test]
    fn test_encode_number() {
        assert_eq!(
            tv(-1).encode(),
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
            assert!(tv(t.0).encode() < tv(t.1).encode(),);
        }
    }

    #[test]
    fn test_encode_string() {
        assert_eq!(
            tv("foo").encode(),
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
                &vec![tv(Rc::new("phones".to_string())), tv(1)],
                &tv("+44 2345678")
            ),
            vec![
                2,  // KEY_INDEX
                0,  // separator
                44, // JsonTag::String
                112, 104, 111, 110, 101, 115, // phones
                1,   // path separator
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
                    tv(Rc::new("pets".to_string())),
                    tv(Rc::new("bennie".to_string())),
                    tv(Rc::new("age".to_string())),
                ],
                &tv(9),
            ),
            vec![
                2,  // KEY_INDEX
                0,  // separator
                44, //String
                112, 101, 116, 115, // pets
                1,   // path sep
                44,  // String
                98, 101, 110, 110, 105, 101, // bennie
                1,   // path sep
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

    #[test]
    fn test_encode_array_key() {
        assert_eq!(
            encode_index_key(&"foo".to_string(), &keypath!["pet", 1], &tv("cat")),
            vec![
                2, 0, // index key
                44, 112, 101, 116, 1, // string pet
                43, 191, 240, 0, 0, 0, 0, 0, 0, 0, // number 1.0
                44, 99, 97, 116, 0, // string cat
                44, 102, 111, 111 // string foo
            ],
        )
    }
    #[test]
    fn test_index_key_path_prefix() {
        let k = encode_index_key(&"foo".to_string(), &keypath!["pet", 1], &tv("cat"));
        let expected = vec![
            2, 0, // index key
            44, 112, 101, 116, 1, // string pet + PATH SEP
            43, 191, 240, 0, 0, 0, 0, 0, 0, // number 1.0 (NO SEP)
        ];
        assert_eq!(path_length(&k[2..]), expected.len() - 2);
        assert_eq!(k.path_prefix().unwrap(), &expected[2..]);
    }

    #[test]
    fn test_index_key_path_prefix2() {
        let k = encode_index_key(
            &"foo".to_string(),
            &keypath!["pet", "pet", "pet", "pet", 1],
            &tv("cat"),
        );
        let expected = vec![
            2, 0, // index key
            44, 112, 101, 116, 1, // string pet + PATH SEP
            44, 112, 101, 116, 1, // string pet + PATH SEP
            44, 112, 101, 116, 1, // string pet + PATH SEP
            44, 112, 101, 116, 1, // string pet + PATH SEP
            43, 191, 240, 0, 0, 0, 0, 0, 0, // number 1.0 (NO SEP)
        ];
        assert_eq!(path_length(&k[2..]), expected.len() - 2);
        assert_eq!(k.path_prefix().unwrap(), &expected[2..]);
    }
}
