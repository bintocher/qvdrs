use crate::error::{QvdError, QvdResult};

#[derive(Debug, Clone)]
pub struct QvdTableHeader {
    pub qv_build_no: String,
    pub creator_doc: String,
    pub create_utc_time: String,
    pub source_create_utc_time: String,
    pub source_file_utc_time: String,
    pub source_file_size: String,
    pub stale_utc_time: String,
    pub table_name: String,
    pub fields: Vec<QvdFieldHeader>,
    pub compression: String,
    pub record_byte_size: usize,
    pub no_of_records: usize,
    pub offset: usize,
    pub length: usize,
    pub lineage: Vec<LineageInfo>,
    pub comment: String,
}

#[derive(Debug, Clone)]
pub struct QvdFieldHeader {
    pub field_name: String,
    pub bit_offset: usize,
    pub bit_width: usize,
    pub bias: i32,
    pub number_format: NumberFormat,
    pub no_of_symbols: usize,
    pub offset: usize,
    pub length: usize,
    pub comment: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NumberFormat {
    pub format_type: String,
    pub n_dec: i32,
    pub use_thou: i32,
    pub fmt: String,
    pub dec: String,
    pub thou: String,
}

impl Default for NumberFormat {
    fn default() -> Self {
        NumberFormat {
            format_type: "UNKNOWN".to_string(),
            n_dec: 0,
            use_thou: 0,
            fmt: String::new(),
            dec: String::new(),
            thou: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LineageInfo {
    pub discriminator: String,
    pub statement: String,
}

/// Extract text content between `<tag>` and `</tag>` from XML string.
/// Returns empty string if tag is not found or is self-closing.
fn xml_tag_value(xml: &str, tag: &str) -> String {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    if let Some(start) = xml.find(&open) {
        let content_start = start + open.len();
        if let Some(end) = xml[content_start..].find(&close) {
            let val = &xml[content_start..content_start + end];
            return xml_unescape(val.trim());
        }
    }
    String::new()
}

/// Parse integer from XML tag, default to 0.
fn xml_tag_int(xml: &str, tag: &str) -> i32 {
    let s = xml_tag_value(xml, tag);
    s.parse().unwrap_or(0)
}

/// Parse usize from XML tag, default to 0.
fn xml_tag_usize(xml: &str, tag: &str) -> usize {
    let s = xml_tag_value(xml, tag);
    s.parse().unwrap_or(0)
}

/// Unescape basic XML entities.
fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

/// Escape basic XML entities for writing.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

/// Find all occurrences of a block between `<tag>` and `</tag>`.
fn xml_find_blocks<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let mut blocks = Vec::new();
    let mut search_from = 0;
    while let Some(start) = xml[search_from..].find(&open) {
        let abs_start = search_from + start;
        let content_start = abs_start + open.len();
        if let Some(end) = xml[content_start..].find(&close) {
            blocks.push(&xml[content_start..content_start + end]);
            search_from = content_start + end + close.len();
        } else {
            break;
        }
    }
    blocks
}

fn parse_number_format(xml: &str) -> NumberFormat {
    NumberFormat {
        format_type: xml_tag_value(xml, "Type"),
        n_dec: xml_tag_int(xml, "nDec"),
        use_thou: xml_tag_int(xml, "UseThou"),
        fmt: xml_tag_value(xml, "Fmt"),
        dec: xml_tag_value(xml, "Dec"),
        thou: xml_tag_value(xml, "Thou"),
    }
}

fn parse_tags(xml: &str) -> Vec<String> {
    let blocks = xml_find_blocks(xml, "String");
    blocks.iter().map(|b| xml_unescape(b.trim())).collect()
}

fn parse_field_header(xml: &str) -> QvdFieldHeader {
    let number_format = xml_find_blocks(xml, "NumberFormat")
        .first()
        .map(|b| parse_number_format(b))
        .unwrap_or_default();

    let tags_block = xml_find_blocks(xml, "Tags");
    let tags = tags_block
        .first()
        .map(|b| parse_tags(b))
        .unwrap_or_default();

    QvdFieldHeader {
        field_name: xml_tag_value(xml, "FieldName"),
        bit_offset: xml_tag_usize(xml, "BitOffset"),
        bit_width: xml_tag_usize(xml, "BitWidth"),
        bias: xml_tag_int(xml, "Bias"),
        number_format,
        no_of_symbols: xml_tag_usize(xml, "NoOfSymbols"),
        offset: xml_tag_usize(xml, "Offset"),
        length: xml_tag_usize(xml, "Length"),
        comment: xml_tag_value(xml, "Comment"),
        tags,
    }
}

fn parse_lineage(xml: &str) -> Vec<LineageInfo> {
    xml_find_blocks(xml, "LineageInfo")
        .iter()
        .map(|b| LineageInfo {
            discriminator: xml_tag_value(b, "Discriminator"),
            statement: xml_tag_value(b, "Statement"),
        })
        .collect()
}

/// Parse the XML header string into a `QvdTableHeader`.
pub fn parse_xml_header(xml: &str) -> QvdResult<QvdTableHeader> {
    let root = xml_find_blocks(xml, "QvdTableHeader");
    let root = root.first().ok_or_else(|| {
        QvdError::Xml("Missing <QvdTableHeader> root element".to_string())
    })?;

    let fields_blocks = xml_find_blocks(root, "QvdFieldHeader");
    let fields: Vec<QvdFieldHeader> = fields_blocks
        .iter()
        .map(|b| parse_field_header(b))
        .collect();

    let lineage_block = xml_find_blocks(root, "Lineage");
    let lineage = lineage_block
        .first()
        .map(|b| parse_lineage(b))
        .unwrap_or_default();

    Ok(QvdTableHeader {
        qv_build_no: xml_tag_value(root, "QvBuildNo"),
        creator_doc: xml_tag_value(root, "CreatorDoc"),
        create_utc_time: xml_tag_value(root, "CreateUtcTime"),
        source_create_utc_time: xml_tag_value(root, "SourceCreateUtcTime"),
        source_file_utc_time: xml_tag_value(root, "SourceFileUtcTime"),
        source_file_size: xml_tag_value(root, "SourceFileSize"),
        stale_utc_time: xml_tag_value(root, "StaleUtcTime"),
        table_name: xml_tag_value(root, "TableName"),
        fields,
        compression: xml_tag_value(root, "Compression"),
        record_byte_size: xml_tag_usize(root, "RecordByteSize"),
        no_of_records: xml_tag_usize(root, "NoOfRecords"),
        // Table-level Offset/Length must be parsed from AFTER </Fields> to avoid
        // picking up field-level Offset/Length values inside <QvdFieldHeader> elements.
        offset: {
            let after_fields = root.rfind("</Fields>")
                .map(|pos| &root[pos..])
                .unwrap_or(root);
            xml_tag_usize(after_fields, "Offset")
        },
        length: {
            let after_fields = root.rfind("</Fields>")
                .map(|pos| &root[pos..])
                .unwrap_or(root);
            xml_tag_usize(after_fields, "Length")
        },
        lineage,
        comment: xml_tag_value(root, "Comment"),
    })
}

/// Serialize a `QvdTableHeader` back to XML string (with \r\n line endings).
pub fn write_xml_header(header: &QvdTableHeader) -> String {
    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n");
    xml.push_str(" <QvdTableHeader>\r\n");
    write_tag(&mut xml, 3, "QvBuildNo", &header.qv_build_no);
    write_tag(&mut xml, 3, "CreatorDoc", &header.creator_doc);
    write_tag(&mut xml, 3, "CreateUtcTime", &header.create_utc_time);
    write_tag(&mut xml, 3, "SourceCreateUtcTime", &header.source_create_utc_time);
    write_tag(&mut xml, 3, "SourceFileUtcTime", &header.source_file_utc_time);
    write_tag(&mut xml, 3, "SourceFileSize", &header.source_file_size);
    write_tag(&mut xml, 3, "StaleUtcTime", &header.stale_utc_time);
    write_tag(&mut xml, 3, "TableName", &header.table_name);
    xml.push_str("   <Fields>\r\n");
    for field in &header.fields {
        write_field_header(&mut xml, field);
    }
    xml.push_str("   </Fields>\r\n");
    write_tag(&mut xml, 3, "Compression", &header.compression);
    write_tag(&mut xml, 3, "RecordByteSize", &header.record_byte_size.to_string());
    write_tag(&mut xml, 3, "NoOfRecords", &header.no_of_records.to_string());
    write_tag(&mut xml, 3, "Offset", &header.offset.to_string());
    write_tag(&mut xml, 3, "Length", &header.length.to_string());

    if !header.lineage.is_empty() {
        xml.push_str("   <Lineage>\r\n");
        for li in &header.lineage {
            xml.push_str("     <LineageInfo>\r\n");
            write_tag(&mut xml, 7, "Discriminator", &xml_escape(&li.discriminator));
            write_tag(&mut xml, 7, "Statement", &xml_escape(&li.statement));
            xml.push_str("     </LineageInfo>\r\n");
        }
        xml.push_str("   </Lineage>\r\n");
    } else {
        write_tag(&mut xml, 3, "Lineage", "");
    }

    write_tag(&mut xml, 3, "Comment", &header.comment);
    write_tag(&mut xml, 3, "EncryptionInfo", "");
    write_tag(&mut xml, 3, "TableTags", "");
    write_tag(&mut xml, 3, "ProfilingData", "");
    xml.push_str(" </QvdTableHeader>\r\n");
    xml
}

fn write_tag(xml: &mut String, indent: usize, tag: &str, value: &str) {
    for _ in 0..indent {
        xml.push(' ');
    }
    xml.push('<');
    xml.push_str(tag);
    xml.push('>');
    xml.push_str(value);
    xml.push_str("</");
    xml.push_str(tag);
    xml.push_str(">\r\n");
}

fn write_field_header(xml: &mut String, field: &QvdFieldHeader) {
    xml.push_str("     <QvdFieldHeader>\r\n");
    write_tag(xml, 7, "FieldName", &xml_escape(&field.field_name));
    write_tag(xml, 7, "BitOffset", &field.bit_offset.to_string());
    write_tag(xml, 7, "BitWidth", &field.bit_width.to_string());
    write_tag(xml, 7, "Bias", &field.bias.to_string());

    xml.push_str("       <NumberFormat>\r\n");
    write_tag(xml, 9, "Type", &field.number_format.format_type);
    write_tag(xml, 9, "nDec", &field.number_format.n_dec.to_string());
    write_tag(xml, 9, "UseThou", &field.number_format.use_thou.to_string());
    write_tag(xml, 9, "Fmt", &field.number_format.fmt);
    write_tag(xml, 9, "Dec", &field.number_format.dec);
    write_tag(xml, 9, "Thou", &field.number_format.thou);
    xml.push_str("       </NumberFormat>\r\n");

    write_tag(xml, 7, "NoOfSymbols", &field.no_of_symbols.to_string());
    write_tag(xml, 7, "Offset", &field.offset.to_string());
    write_tag(xml, 7, "Length", &field.length.to_string());
    write_tag(xml, 7, "Comment", &field.comment);

    if !field.tags.is_empty() {
        xml.push_str("       <Tags>\r\n");
        for tag in &field.tags {
            write_tag(xml, 9, "String", tag);
        }
        xml.push_str("       </Tags>\r\n");
    }

    xml.push_str("     </QvdFieldHeader>\r\n");
}
