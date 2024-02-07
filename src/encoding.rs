use std::rc::Rc;

enum JsonTag {
    // printable character makes easier debugging
    Null = 0x28,   // char: (
    False = 0x29,  // char: )
    True = 0x2a,   // char: *
    Number = 0x2b, // char: +
    String = 0x2c, // char: ,
}

#[derive(Clone, Debug)]
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
}
