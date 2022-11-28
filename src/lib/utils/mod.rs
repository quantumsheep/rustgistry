use serde::Serialize;

use crate::storage::Error;

pub fn to_json_normalized<T>(value: &T) -> Result<String, Error>
where
    T: ?Sized + Serialize,
{
    let buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"   ");
    let mut ser = serde_json::Serializer::with_formatter(buf, formatter);

    value.serialize(&mut ser)?;

    let s = String::from_utf8(ser.into_inner())?;
    Ok(s)
}
