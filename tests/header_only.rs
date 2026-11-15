use std::io::Cursor;
use qvd::read_qvd_header;

const XML: &str = "<QvdTableHeader><CreatorDoc>creator</CreatorDoc><CreateUtcTime>2026-11-16T00:00:00Z</CreateUtcTime><TableName>test</TableName><NoOfRecords>999999999</NoOfRecords><Fields><QvdFieldHeader><FieldName>pk</FieldName><NoOfSymbols>10</NoOfSymbols><BitWidth>4</BitWidth><Comment>Key formula</Comment></QvdFieldHeader></Fields></QvdTableHeader>";

#[test]
fn reads_metadata_without_touching_invalid_record_data() {
    let mut bytes = XML.as_bytes().to_vec();
    bytes.push(0);
    bytes.extend_from_slice(b"not a symbol or index table");
    let mut cursor = Cursor::new(bytes);
    let header = read_qvd_header(&mut cursor).unwrap();
    assert_eq!(cursor.position(), (XML.len() + 1) as u64);
    assert_eq!(header.no_of_records, 999999999);
    assert_eq!(header.creator_doc, "creator");
    assert_eq!(header.fields[0].comment, "Key formula");
}

#[test]
fn rejects_truncated_and_unterminated_headers() {
    assert!(read_qvd_header(Cursor::new(XML.as_bytes())).is_err());
    assert!(read_qvd_header(Cursor::new(b"<QvdTableHeader>\0")).is_err());
}

#[test]
fn accepts_bom_and_crlf_before_separator() {
    let bytes = format!("\u{feff}{}\r\n\0", XML).into_bytes();
    assert_eq!(read_qvd_header(Cursor::new(bytes)).unwrap().table_name, "test");
}
