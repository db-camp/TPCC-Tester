//! RMDB Wire Protocol v3 字节层（赛题附件 A §1–4）。
//!
//! 只负责 frame / payload 的编解码，不做 socket IO；异步读写循环见 `client.rs`。
//! 所有多字节整数均为大端序。

use crate::error::TpccError;

/// 握手报文：ASCII "RMDB" + major=3 + minor=0，共 8 字节，服务端必须原样回送。
pub const HANDSHAKE: [u8; 8] = [b'R', b'M', b'D', b'B', 0x00, 0x03, 0x00, 0x00];

/// 单个请求或响应 payload 上限（1 MiB）。
pub const MAX_PAYLOAD: usize = 1024 * 1024;

/// ERROR / TRANSACTION_ABORT 诊断信息上限（64 KiB）。
pub const MAX_DIAGNOSTIC: usize = 64 * 1024;

/// frame header 固定 8 字节：u32 payload_bytes + u8 tag + u8 flags + u16 reserved。
pub const FRAME_HEADER_LEN: usize = 8;

/// frame tag（附件 A §2）。
pub mod tag {
    pub const META: u8 = 0x01;
    pub const ROW: u8 = 0x02;
    pub const COMMAND_OK: u8 = 0x10;
    pub const RESULT_END: u8 = 0x11;
    pub const TRANSACTION_ABORT: u8 = 0x12;
    pub const ERROR: u8 = 0x13;
    pub const PREPARE_OK: u8 = 0x14;
    pub const BATCH_RESULT: u8 = 0x15;
    pub const EXEC_STREAM: u8 = 0x20;
    pub const PREPARE_SET: u8 = 0x21;
    pub const EXEC_BATCH: u8 = 0x22;

    pub fn name(t: u8) -> &'static str {
        match t {
            META => "META",
            ROW => "ROW",
            COMMAND_OK => "COMMAND_OK",
            RESULT_END => "RESULT_END",
            TRANSACTION_ABORT => "TRANSACTION_ABORT",
            ERROR => "ERROR",
            PREPARE_OK => "PREPARE_OK",
            BATCH_RESULT => "BATCH_RESULT",
            EXEC_STREAM => "EXEC_STREAM",
            PREPARE_SET => "PREPARE_SET",
            EXEC_BATCH => "EXEC_BATCH",
            _ => "UNKNOWN",
        }
    }
}

fn perr(msg: impl Into<String>) -> TpccError {
    TpccError::Protocol(msg.into())
}

/// SQL 列类型（附件 A §3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Int32 = 0x01,
    Float32 = 0x02,
    Char = 0x03,
}

impl SqlType {
    pub fn from_u8(v: u8) -> Result<Self, TpccError> {
        match v {
            0x01 => Ok(SqlType::Int32),
            0x02 => Ok(SqlType::Float32),
            0x03 => Ok(SqlType::Char),
            other => Err(perr(format!("非法 SQL typetag: {other:#04x}"))),
        }
    }
}

/// META 中的列定义。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub sql_type: SqlType,
}

/// 一个类型化 cell 的值。present=0 表示 NULL（协议层保留编码，正式负载不写入 NULL）。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int(i32),
    Float(f32),
    Char(String),
}

impl Value {
    /// NULL 判定；FLOAT bit-pattern 路径（#16）与一致性检查（#15）使用
    #[allow(dead_code)]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// 整数视图。CHAR（legacy 文本协议的 cell）会尝试解析；空串/NULL 返回 None。
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Int(v) => Some(*v),
            Value::Char(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v as i64),
            Value::Char(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    /// 浮点视图。FLOAT32 精确扩展为 f64；INT32 允许升宽（聚合结果可能按整数返回）。
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v as f64),
            Value::Int(v) => Some(*v as f64),
            Value::Char(s) => s.trim().parse().ok(),
            Value::Null => None,
        }
    }

    /// f32 视图；FLOAT32 bit-pattern 核对路径（#16）使用
    #[allow(dead_code)]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Value::Float(v) => Some(*v),
            Value::Int(v) => Some(*v as f32),
            Value::Char(s) => s.trim().parse().ok(),
            Value::Null => None,
        }
    }

    /// 字符串视图；非 CHAR 返回空串。
    pub fn as_str(&self) -> &str {
        match self {
            Value::Char(s) => s.as_str(),
            _ => "",
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Value::Null => String::new(),
            Value::Int(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::Char(v) => v.clone(),
        };
        f.pad(&s)
    }
}

/// 8 字节通用 frame header。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub payload_len: usize,
    pub tag: u8,
    pub flags: u8,
}

impl FrameHeader {
    /// 解码 header 并校验 reserved=0、payload 不超过 1 MiB（先校验长度再分配内存）。
    pub fn decode(buf: &[u8; FRAME_HEADER_LEN]) -> Result<Self, TpccError> {
        let payload_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let tag = buf[4];
        let flags = buf[5];
        let reserved = u16::from_be_bytes([buf[6], buf[7]]);
        if reserved != 0 {
            return Err(perr(format!("frame reserved 必须为 0, 实际 {reserved:#06x}")));
        }
        if payload_len > MAX_PAYLOAD {
            return Err(perr(format!(
                "payload {payload_len} 字节超过 1 MiB 上限 (tag={})",
                tag::name(tag)
            )));
        }
        Ok(Self {
            payload_len,
            tag,
            flags,
        })
    }
}

/// 编码一个完整请求 frame（header + payload）。
pub fn encode_frame(t: u8, flags: u8, payload: &[u8]) -> Result<Vec<u8>, TpccError> {
    if payload.len() > MAX_PAYLOAD {
        return Err(perr(format!(
            "请求 payload {} 字节超过 1 MiB 上限",
            payload.len()
        )));
    }
    let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.push(t);
    buf.push(flags);
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(payload);
    Ok(buf)
}

/// 编码 EXEC_STREAM 请求：payload 为一条非空、不含 NUL 的 UTF-8 SQL。
pub fn encode_exec_stream(sql: &str) -> Result<Vec<u8>, TpccError> {
    if sql.is_empty() {
        return Err(perr("EXEC_STREAM SQL 不能为空"));
    }
    if sql.bytes().any(|b| b == 0) {
        return Err(perr("EXEC_STREAM SQL 不能包含 NUL 字节"));
    }
    encode_frame(tag::EXEC_STREAM, 0, sql.as_bytes())
}

/// 顺序读取 payload 字段的游标；所有读取都做边界检查。
struct PayloadReader<'a> {
    buf: &'a [u8],
    pos: usize,
    ctx: &'static str,
}

impl<'a> PayloadReader<'a> {
    fn new(buf: &'a [u8], ctx: &'static str) -> Self {
        Self { buf, pos: 0, ctx }
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8], TpccError> {
        let end = self.pos.checked_add(n).ok_or_else(|| {
            perr(format!("{} payload 长度字段溢出", self.ctx))
        })?;
        if end > self.buf.len() {
            return Err(perr(format!(
                "{} payload 被截断: 需要 {} 字节, 剩余 {}",
                self.ctx,
                n,
                self.buf.len() - self.pos
            )));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, TpccError> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, TpccError> {
        let b = self.bytes(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, TpccError> {
        let b = self.bytes(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64, TpccError> {
        let b = self.bytes(8)?;
        Ok(u64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn i32(&mut self) -> Result<i32, TpccError> {
        Ok(self.u32()? as i32)
    }

    /// payload 尾随未解释字节属于协议错误。
    fn expect_end(&self) -> Result<(), TpccError> {
        if self.pos != self.buf.len() {
            return Err(perr(format!(
                "{} payload 尾随 {} 字节未解释数据",
                self.ctx,
                self.buf.len() - self.pos
            )));
        }
        Ok(())
    }
}

/// 解析 META payload：u16 column_count（必须 > 0）+ 列定义序列。
pub fn parse_meta(payload: &[u8]) -> Result<Vec<ColumnDef>, TpccError> {
    let mut r = PayloadReader::new(payload, "META");
    let column_count = r.u16()?;
    if column_count == 0 {
        return Err(perr("META column_count 必须 > 0"));
    }
    let mut columns = Vec::with_capacity(column_count as usize);
    for _ in 0..column_count {
        columns.push(parse_column_def(&mut r)?);
    }
    r.expect_end()?;
    Ok(columns)
}

fn parse_column_def(r: &mut PayloadReader<'_>) -> Result<ColumnDef, TpccError> {
    let name_bytes = r.u16()?;
    if name_bytes == 0 {
        return Err(perr("列定义 name_bytes 必须 > 0"));
    }
    let name = std::str::from_utf8(r.bytes(name_bytes as usize)?)
        .map_err(|_| perr("列名不是合法 UTF-8"))?
        .to_string();
    let sql_type = SqlType::from_u8(r.u8()?)?;
    Ok(ColumnDef { name, sql_type })
}

/// 按列 schema 解析一个 ROW payload；cell 数与列数一致，不携带行长度。
pub fn parse_row(payload: &[u8], columns: &[ColumnDef]) -> Result<Vec<Value>, TpccError> {
    let mut r = PayloadReader::new(payload, "ROW");
    let mut cells = Vec::with_capacity(columns.len());
    for col in columns {
        cells.push(parse_cell(&mut r, col.sql_type)?);
    }
    r.expect_end()?;
    Ok(cells)
}

fn parse_cell(r: &mut PayloadReader<'_>, sql_type: SqlType) -> Result<Value, TpccError> {
    let present = r.u8()?;
    match present {
        0 => Ok(Value::Null),
        1 => match sql_type {
            SqlType::Int32 => Ok(Value::Int(r.i32()?)),
            SqlType::Float32 => Ok(Value::Float(f32::from_bits(r.u32()?))),
            SqlType::Char => {
                let byte_count = r.u32()? as usize;
                let s = std::str::from_utf8(r.bytes(byte_count)?)
                    .map_err(|_| perr("CHAR cell 不是合法 UTF-8"))?;
                Ok(Value::Char(s.to_string()))
            }
        },
        other => Err(perr(format!("cell present 只能为 0/1, 实际 {other}"))),
    }
}

/// 解析 RESULT_END payload：u64 row_count。
pub fn parse_result_end(payload: &[u8]) -> Result<u64, TpccError> {
    let mut r = PayloadReader::new(payload, "RESULT_END");
    let row_count = r.u64()?;
    r.expect_end()?;
    Ok(row_count)
}

/// 解析 ERROR / TRANSACTION_ABORT 的 UTF-8 诊断（至多 64 KiB）。
pub fn parse_diagnostic(payload: &[u8]) -> Result<String, TpccError> {
    if payload.len() > MAX_DIAGNOSTIC {
        return Err(perr(format!(
            "诊断信息 {} 字节超过 64 KiB 上限",
            payload.len()
        )));
    }
    std::str::from_utf8(payload)
        .map(|s| s.to_string())
        .map_err(|_| perr("诊断信息不是合法 UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_bytes(payload_len: u32, tag: u8, flags: u8, reserved: u16) -> [u8; 8] {
        let mut b = [0u8; 8];
        b[..4].copy_from_slice(&payload_len.to_be_bytes());
        b[4] = tag;
        b[5] = flags;
        b[6..].copy_from_slice(&reserved.to_be_bytes());
        b
    }

    #[test]
    fn handshake_layout() {
        assert_eq!(&HANDSHAKE[..4], b"RMDB");
        assert_eq!(u16::from_be_bytes([HANDSHAKE[4], HANDSHAKE[5]]), 3);
        assert_eq!(u16::from_be_bytes([HANDSHAKE[6], HANDSHAKE[7]]), 0);
    }

    #[test]
    fn frame_header_roundtrip() {
        let h = FrameHeader::decode(&header_bytes(42, tag::META, 0, 0)).unwrap();
        assert_eq!(h.payload_len, 42);
        assert_eq!(h.tag, tag::META);
        assert_eq!(h.flags, 0);
    }

    #[test]
    fn frame_header_rejects_nonzero_reserved() {
        assert!(FrameHeader::decode(&header_bytes(0, tag::COMMAND_OK, 0, 1)).is_err());
    }

    #[test]
    fn frame_header_rejects_oversized_payload() {
        let err = FrameHeader::decode(&header_bytes(
            MAX_PAYLOAD as u32 + 1,
            tag::ROW,
            0,
            0,
        ))
        .unwrap_err();
        assert!(err.to_string().contains("1 MiB"));
    }

    #[test]
    fn encode_exec_stream_frame() {
        let f = encode_exec_stream("show tables;").unwrap();
        assert_eq!(&f[..4], &12u32.to_be_bytes());
        assert_eq!(f[4], tag::EXEC_STREAM);
        assert_eq!(f[5], 0);
        assert_eq!(&f[6..8], &[0, 0]);
        assert_eq!(&f[8..], b"show tables;");
    }

    #[test]
    fn encode_exec_stream_rejects_empty_and_nul() {
        assert!(encode_exec_stream("").is_err());
        assert!(encode_exec_stream("select 1\0;").is_err());
    }

    #[test]
    fn encode_exec_stream_rejects_oversized_sql() {
        let sql = "x".repeat(MAX_PAYLOAD + 1);
        assert!(encode_exec_stream(&sql).is_err());
    }

    fn meta_payload(cols: &[(&str, u8)]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&(cols.len() as u16).to_be_bytes());
        for (name, t) in cols {
            p.extend_from_slice(&(name.len() as u16).to_be_bytes());
            p.extend_from_slice(name.as_bytes());
            p.push(*t);
        }
        p
    }

    #[test]
    fn parse_meta_ok() {
        let cols = parse_meta(&meta_payload(&[("d_next_o_id", 0x01), ("d_tax", 0x02)])).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "d_next_o_id");
        assert_eq!(cols[0].sql_type, SqlType::Int32);
        assert_eq!(cols[1].sql_type, SqlType::Float32);
    }

    #[test]
    fn parse_meta_rejects_zero_columns() {
        assert!(parse_meta(&0u16.to_be_bytes()).is_err());
    }

    #[test]
    fn parse_meta_rejects_empty_name() {
        let mut p = Vec::new();
        p.extend_from_slice(&1u16.to_be_bytes());
        p.extend_from_slice(&0u16.to_be_bytes()); // name_bytes = 0
        p.push(0x01);
        assert!(parse_meta(&p).is_err());
    }

    #[test]
    fn parse_meta_rejects_unknown_type() {
        assert!(parse_meta(&meta_payload(&[("c", 0x04)])).is_err());
    }

    #[test]
    fn parse_meta_rejects_trailing_bytes() {
        let mut p = meta_payload(&[("c", 0x01)]);
        p.push(0xff);
        assert!(parse_meta(&p).is_err());
    }

    #[test]
    fn parse_meta_rejects_truncation() {
        let p = meta_payload(&[("c_last", 0x03)]);
        assert!(parse_meta(&p[..p.len() - 2]).is_err());
    }

    fn cols(types: &[SqlType]) -> Vec<ColumnDef> {
        types
            .iter()
            .map(|t| ColumnDef {
                name: "c".to_string(),
                sql_type: *t,
            })
            .collect()
    }

    #[test]
    fn parse_row_typed_cells() {
        let mut p = Vec::new();
        // INT32 = -7
        p.push(1);
        p.extend_from_slice(&(-7i32).to_be_bytes());
        // FLOAT32 = 3.5，按 bit pattern
        p.push(1);
        p.extend_from_slice(&3.5f32.to_bits().to_be_bytes());
        // CHAR = "BC"（无 padding、无 NUL）
        p.push(1);
        p.extend_from_slice(&2u32.to_be_bytes());
        p.extend_from_slice(b"BC");
        // NULL
        p.push(0);

        let row = parse_row(
            &p,
            &cols(&[SqlType::Int32, SqlType::Float32, SqlType::Char, SqlType::Int32]),
        )
        .unwrap();
        assert_eq!(row[0], Value::Int(-7));
        assert_eq!(row[1], Value::Float(3.5));
        assert_eq!(row[2], Value::Char("BC".to_string()));
        assert_eq!(row[3], Value::Null);
    }

    #[test]
    fn parse_row_preserves_float_bit_pattern() {
        // -0.0 与 +0.0 数值相等但位型不同，wire 层必须原样保留
        let mut p = Vec::new();
        p.push(1);
        p.extend_from_slice(&(-0.0f32).to_bits().to_be_bytes());
        let row = parse_row(&p, &cols(&[SqlType::Float32])).unwrap();
        match row[0] {
            Value::Float(f) => assert_eq!(f.to_bits(), (-0.0f32).to_bits()),
            _ => panic!("期望 Float cell"),
        }
    }

    #[test]
    fn parse_row_rejects_bad_present() {
        let p = vec![2u8];
        assert!(parse_row(&p, &cols(&[SqlType::Int32])).is_err());
    }

    #[test]
    fn parse_row_rejects_truncated_cell() {
        let p = vec![1u8, 0, 0]; // INT32 只有 2 字节
        assert!(parse_row(&p, &cols(&[SqlType::Int32])).is_err());
    }

    #[test]
    fn parse_row_rejects_trailing_bytes() {
        let mut p = Vec::new();
        p.push(1);
        p.extend_from_slice(&1i32.to_be_bytes());
        p.push(0xee);
        assert!(parse_row(&p, &cols(&[SqlType::Int32])).is_err());
    }

    #[test]
    fn parse_row_rejects_char_length_beyond_payload() {
        let mut p = Vec::new();
        p.push(1);
        p.extend_from_slice(&100u32.to_be_bytes());
        p.extend_from_slice(b"short");
        assert!(parse_row(&p, &cols(&[SqlType::Char])).is_err());
    }

    #[test]
    fn parse_result_end_ok_and_rejects_bad_size() {
        assert_eq!(parse_result_end(&5u64.to_be_bytes()).unwrap(), 5);
        assert!(parse_result_end(&[0u8; 4]).is_err());
        assert!(parse_result_end(&[0u8; 9]).is_err());
    }

    #[test]
    fn parse_diagnostic_enforces_limit_and_utf8() {
        assert_eq!(parse_diagnostic(b"dup key").unwrap(), "dup key");
        assert!(parse_diagnostic(&vec![b'x'; MAX_DIAGNOSTIC + 1]).is_err());
        assert!(parse_diagnostic(&[0xff, 0xfe]).is_err());
    }

    #[test]
    fn value_accessors() {
        assert_eq!(Value::Int(42).as_i64(), Some(42));
        assert_eq!(Value::Char("42".into()).as_i32(), Some(42));
        assert_eq!(Value::Char("".into()).as_i32(), None);
        assert_eq!(Value::Null.as_i32(), None);
        assert_eq!(Value::Float(1.5).as_f64(), Some(1.5));
        assert_eq!(Value::Char("3.25".into()).as_f64(), Some(3.25));
        assert_eq!(Value::Char("BC".into()).as_str(), "BC");
        assert!(Value::Null.is_null());
        assert_eq!(format!("{:>5}", Value::Int(7)), "    7");
    }
}
