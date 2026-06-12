//! Wire framing — inner batch `[count u16 LE][items…]`; item content is opaque.

use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use std::io::IoSlice;
use tokio::io::AsyncWriteExt;

/// Consumer → master: pede batch (max_items + tag).
pub const MAGIC_CONSUMER_READY: [u8; 4] = *b"CRDY";
pub const MAGIC_PUB: [u8; 4] = *b"PUB!";
/// Ingress batch — legado proxy; master não recebe payload.
pub const MAGIC_PUB_BATCH: [u8; 4] = *b"PUBB";
pub const MAGIC_BATCH: [u8; 4] = *b"BATC";
pub const MAGIC_ERR: [u8; 4] = *b"ERR!";
/// Producer → master: ring local tem itens.
pub const MAGIC_PRODUCER_READY: [u8; 4] = *b"PRDY";
/// Consumer → master (1º frame): endpoint dados TCP `host:port`.
pub const MAGIC_REGISTER: [u8; 4] = *b"REG!";
/// Master → producer: entrega pedido CRDY + destino dados TCP.
pub const MAGIC_ASSIGN: [u8; 4] = *b"ASGN";
/// Producer → master: batch TCP entregue.
pub const MAGIC_DELIVERED: [u8; 4] = *b"DELV";
/// Producer → master: entrega falhou (itens requeued localmente).
pub const MAGIC_FAILED: [u8; 4] = *b"FAIL";
/// Producer → master: backlog local (ring + pending).
pub const MAGIC_HEARTBEAT: [u8; 4] = *b"HBRP";

pub const MAX_INGRESS_ITEM_BYTES: usize = 32 * 1024 * 1024;
/// Frame PUBB completo (header + inner batch).
pub const MAX_INGRESS_BATCH_FRAME_BYTES: usize = 64 * 1024 * 1024;

pub const ERR_BUSY: u16 = 1;
pub const ERR_PROTOCOL: u16 = 2;

/// Batches acima disso: confiar em `write_vectored` sem `flush` extra.
pub const FLUSH_BELOW_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct ReadyRequest {
    pub max_items: u16,
    /// Optional consumer identifier; empty → master uses `consumer-{id}`.
    pub consumer_tag: String,
}

#[derive(Debug, Clone)]
pub struct AssignRequest {
    pub max_items: u16,
    pub consumer_tag: String,
    /// Endpoint TCP data plane do consumer (`host:port` para BATC).
    pub data_addr: String,
}

#[derive(Debug, Clone)]
pub struct DeliveryReport {
    pub msg_count: u16,
    pub batch_bytes: u32,
}

#[derive(Debug, Clone)]
pub struct FailureReport {
    pub msg_count: u16,
    pub batch_bytes: u32,
    pub code: u16,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProducerHeartbeat {
    pub pending_messages: u32,
    pub pending_bytes: u32,
    pub ring_messages: u32,
    pub ring_bytes: u32,
    pub drops_total: u64,
}

fn inner_batch_payload_len(items: &[impl AsRef<[u8]>]) -> usize {
    2 + items.iter().map(|i| 4 + i.as_ref().len()).sum::<usize>()
}

/// Inner batch: `[count u16 LE][len u32 LE][item]…` (sem magic).
pub fn encode_batch_payload(items: &[impl AsRef<[u8]>]) -> Vec<u8> {
    let payload_len = inner_batch_payload_len(items);
    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&(items.len() as u16).to_le_bytes());
    for item in items {
        let b = item.as_ref();
        payload.extend_from_slice(&(b.len() as u32).to_le_bytes());
        payload.extend_from_slice(b);
    }
    payload
}

fn encode_framed_batch(magic: [u8; 4], items: &[impl AsRef<[u8]>]) -> Vec<u8> {
    let payload = encode_batch_payload(items);
    let mut frame = Vec::with_capacity(4 + 2 + 4 + payload.len());
    frame.extend_from_slice(&magic);
    frame.extend_from_slice(&(items.len() as u16).to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Monta frame BATC contíguo (tests / fallback).
pub fn encode_batch(items: &[impl AsRef<[u8]>]) -> Vec<u8> {
    encode_framed_batch(MAGIC_BATCH, items)
}

async fn write_framed_batch_vectored(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    magic: [u8; 4],
    items: &[impl AsRef<[u8]>],
) -> Result<u64> {
    let count = items.len();
    let payload_len: u32 = inner_batch_payload_len(items) as u32;

    let mut outer_count = (count as u16).to_le_bytes();
    let mut payload_len_bytes = payload_len.to_le_bytes();
    let mut inner_count = outer_count;

    let mut len_bufs = Vec::with_capacity(count);
    for item in items {
        len_bufs.push((item.as_ref().len() as u32).to_le_bytes());
    }

    let mut slices = Vec::with_capacity(4 + count * 2);
    slices.push(IoSlice::new(&magic));
    slices.push(IoSlice::new(&mut outer_count));
    slices.push(IoSlice::new(&mut payload_len_bytes));
    slices.push(IoSlice::new(&mut inner_count));
    for (item, len_buf) in items.iter().zip(len_bufs.iter_mut()) {
        slices.push(IoSlice::new(len_buf));
        slices.push(IoSlice::new(item.as_ref()));
    }

    writer
        .write_vectored(&slices)
        .await
        .context("write_vectored framed batch")?;

    Ok(10 + payload_len as u64)
}

/// Escreve BATC via write_vectored — evita memcpy do batch inteiro.
pub async fn write_batch_vectored(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    items: &[impl AsRef<[u8]>],
) -> Result<u64> {
    write_framed_batch_vectored(writer, MAGIC_BATCH, items).await
}

/// Lê frame BATC completo; retorna inner payload (`[count u16][items…]`).
pub async fn read_batch_frame(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<Vec<u8>> {
    let mut magic = [0u8; 4];
    read_exact_async(reader, &mut magic).await?;
    if magic != MAGIC_BATCH {
        bail!("expected BATC magic");
    }
    let mut hdr = [0u8; 6];
    read_exact_async(reader, &mut hdr).await?;
    let payload_len = u32::from_le_bytes([hdr[2], hdr[3], hdr[4], hdr[5]]) as usize;
    if payload_len > MAX_INGRESS_BATCH_FRAME_BYTES {
        bail!("batch payload too large ({payload_len})");
    }
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        read_exact_async(reader, &mut payload).await?;
    }
    Ok(payload)
}

pub fn batch_wire_bytes(items: &[impl AsRef<[u8]>]) -> u64 {
    (10 + inner_batch_payload_len(items)) as u64
}

pub fn encode_error(code: u16, message: &str) -> Vec<u8> {
    let msg = message.as_bytes();
    let msg_len = msg.len().min(u16::MAX as usize) as u16;
    let mut frame = Vec::with_capacity(4 + 2 + 2 + msg_len as usize);
    frame.extend_from_slice(&MAGIC_ERR);
    frame.extend_from_slice(&code.to_le_bytes());
    frame.extend_from_slice(&msg_len.to_le_bytes());
    frame.extend_from_slice(&msg[..msg_len as usize]);
    frame
}

pub fn parse_ready_header(body: &[u8]) -> Result<(u16, u16)> {
    if body.len() < 4 {
        bail!("CRDY header too short (expected 4 bytes)");
    }
    Ok((
        u16::from_le_bytes([body[0], body[1]]),
        u16::from_le_bytes([body[2], body[3]]),
    ))
}

async fn read_utf8_field(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    len: usize,
    field: &str,
) -> Result<String> {
    if len == 0 {
        return Ok(String::new());
    }
    if len > 256 {
        bail!("CRDY {field} too long");
    }
    let mut buf = vec![0u8; len];
    read_exact_async(reader, &mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Lê magic CRDY + header + tag num único fluxo (menos syscalls).
pub async fn read_consumer_ready_request(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<ReadyRequest> {
    let mut prefix = [0u8; 8];
    read_exact_async(reader, &mut prefix).await?;
    if prefix[..4] != MAGIC_CONSUMER_READY {
        bail!("expected CRDY magic");
    }
    let (max_items, tag_len) = parse_ready_header(&prefix[4..8])?;
    let consumer_tag = read_utf8_field(reader, tag_len as usize, "consumer_tag").await?;
    Ok(ReadyRequest {
        max_items,
        consumer_tag,
    })
}

pub fn parse_ready(body: &[u8]) -> Result<ReadyRequest> {
    if body.len() < 8 {
        bail!("CRDY frame too short");
    }
    if body[..4] != MAGIC_CONSUMER_READY {
        bail!("expected CRDY magic");
    }
    let (max_items, tag_len) = parse_ready_header(&body[4..8])?;
    let tag_len = tag_len as usize;
    if body.len() < 8 + tag_len {
        bail!("CRDY frame too short for consumer_tag");
    }
    let consumer_tag = if tag_len == 0 {
        String::new()
    } else {
        String::from_utf8_lossy(&body[8..8 + tag_len]).into_owned()
    };
    Ok(ReadyRequest {
        max_items,
        consumer_tag,
    })
}

pub fn parse_pub_payload_len(header: &[u8]) -> Result<u32> {
    if header.len() < 8 {
        bail!("PUB header too short");
    }
    Ok(u32::from_le_bytes([header[4], header[5], header[6], header[7]]))
}

pub async fn read_exact_async(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    buf: &mut [u8],
) -> Result<()> {
    tokio::io::AsyncReadExt::read_exact(reader, buf)
        .await
        .context("read_exact")
        .map(|_| ())
}

pub async fn read_pub_item(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<Bytes> {
    let items = read_ingress_items(reader).await?;
    if items.len() != 1 {
        bail!("read_pub_item expected single item, got {}", items.len());
    }
    Ok(items.into_iter().next().unwrap())
}

/// Lê um frame ingress: `PUB!` (1 item) ou `PUBB` (N items, split no ring).
pub async fn read_ingress_items(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<Vec<Bytes>> {
    let mut magic = [0u8; 4];
    read_exact_async(reader, &mut magic).await?;
    match magic {
        MAGIC_PUB => {
            let mut len_buf = [0u8; 4];
            read_exact_async(reader, &mut len_buf).await?;
            let len = u32::from_le_bytes(len_buf) as usize;
            if len == 0 || len > MAX_INGRESS_ITEM_BYTES {
                bail!("invalid PUB payload len {len}");
            }
            let mut payload = vec![0u8; len];
            read_exact_async(reader, &mut payload).await?;
            Ok(vec![Bytes::from(payload)])
        }
        MAGIC_PUB_BATCH => read_pub_batch_body(reader).await,
        _ => bail!("unknown ingress magic {:?}", &magic),
    }
}

async fn read_pub_batch_body(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<Vec<Bytes>> {
    let mut hdr = [0u8; 6];
    read_exact_async(reader, &mut hdr).await?;
    let outer_count = u16::from_le_bytes([hdr[0], hdr[1]]) as usize;
    let payload_len = u32::from_le_bytes([hdr[2], hdr[3], hdr[4], hdr[5]]) as usize;
    if payload_len == 0 || payload_len > MAX_INGRESS_BATCH_FRAME_BYTES {
        bail!("invalid PUBB payload len {payload_len}");
    }
    let mut payload = BytesMut::with_capacity(payload_len);
    payload.resize(payload_len, 0);
    read_exact_async(reader, &mut payload).await?;
    let payload = payload.freeze();
    let items = parse_batch_payload_items(payload)?;
    if items.len() != outer_count {
        bail!(
            "PUBB count mismatch: header {outer_count}, parsed {}",
            items.len()
        );
    }
    Ok(items)
}

/// Inner batch: `[count u16 LE][len u32 LE][item]…` — slices no buffer compartilhado (zero-copy).
pub fn parse_batch_payload_items(payload: Bytes) -> Result<Vec<Bytes>> {
    let count = batch_item_count(&payload)?;
    let mut out = Vec::with_capacity(count);
    let mut offset = 2usize;
    for _ in 0..count {
        if offset + 4 > payload.len() {
            bail!("batch payload truncated at item len");
        }
        let item_len = u32::from_le_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]) as usize;
        offset += 4;
        if item_len == 0 || item_len > MAX_INGRESS_ITEM_BYTES {
            bail!("invalid batch item len {item_len}");
        }
        if offset + item_len > payload.len() {
            bail!("batch payload truncated at item body");
        }
        out.push(payload.slice(offset..offset + item_len));
        offset += item_len;
    }
    Ok(out)
}

/// Monta frame PUBB contíguo (tests / legado ingress).
pub fn encode_pub_batch(items: &[impl AsRef<[u8]>]) -> Vec<u8> {
    encode_framed_batch(MAGIC_PUB_BATCH, items)
}

/// Escreve PUBB via write_vectored — 1 TCP flush para muitos items.
pub async fn write_pub_batch_vectored(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    items: &[impl AsRef<[u8]>],
) -> Result<u64> {
    if items.is_empty() {
        bail!("PUBB requires at least one item");
    }
    if items.len() > u16::MAX as usize {
        bail!("PUBB item count overflow");
    }
    write_framed_batch_vectored(writer, MAGIC_PUB_BATCH, items).await
}

pub fn pub_batch_wire_bytes(items: &[impl AsRef<[u8]>]) -> u64 {
    batch_wire_bytes(items)
}

pub fn batch_item_count(payload: &[u8]) -> Result<usize> {
    if payload.len() < 2 {
        bail!("batch payload too short");
    }
    Ok(u16::from_le_bytes([payload[0], payload[1]]) as usize)
}

pub fn encode_consumer_ready(max_items: u16, consumer_tag: &str) -> Result<Vec<u8>> {
    let tag = consumer_tag.as_bytes();
    if tag.len() > u16::MAX as usize {
        bail!("consumer_tag too long");
    }
    let mut frame = Vec::with_capacity(4 + 4 + tag.len());
    frame.extend_from_slice(&MAGIC_CONSUMER_READY);
    frame.extend_from_slice(&max_items.to_le_bytes());
    frame.extend_from_slice(&(tag.len() as u16).to_le_bytes());
    frame.extend_from_slice(tag);
    Ok(frame)
}

pub fn encode_register(data_addr: &str) -> Result<Vec<u8>> {
    let addr = data_addr.as_bytes();
    if addr.is_empty() || addr.len() > u16::MAX as usize {
        bail!("invalid data_addr len");
    }
    let mut frame = Vec::with_capacity(4 + 2 + addr.len());
    frame.extend_from_slice(&MAGIC_REGISTER);
    frame.extend_from_slice(&(addr.len() as u16).to_le_bytes());
    frame.extend_from_slice(addr);
    Ok(frame)
}

pub async fn read_register(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<String> {
    let mut magic = [0u8; 4];
    read_exact_async(reader, &mut magic).await?;
    if magic != MAGIC_REGISTER {
        bail!("expected REG! magic");
    }
    read_register_payload(reader).await
}

/// Corpo de `REG!` após os 4 bytes de magic já consumidos.
pub async fn read_register_payload(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<String> {
    let mut len_buf = [0u8; 2];
    read_exact_async(reader, &mut len_buf).await?;
    let len = u16::from_le_bytes(len_buf) as usize;
    if len == 0 || len > 256 {
        bail!("invalid REG data_addr len {len}");
    }
    let mut buf = vec![0u8; len];
    read_exact_async(reader, &mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

pub fn encode_assign(req: &AssignRequest) -> Result<Vec<u8>> {
    let tag = req.consumer_tag.as_bytes();
    let data = req.data_addr.as_bytes();
    if tag.len() > u16::MAX as usize || data.is_empty() || data.len() > u16::MAX as usize {
        bail!("invalid assign field len");
    }
    let mut frame = Vec::with_capacity(4 + 2 + 2 + tag.len() + 2 + data.len());
    frame.extend_from_slice(&MAGIC_ASSIGN);
    frame.extend_from_slice(&req.max_items.to_le_bytes());
    frame.extend_from_slice(&(tag.len() as u16).to_le_bytes());
    frame.extend_from_slice(tag);
    frame.extend_from_slice(&(data.len() as u16).to_le_bytes());
    frame.extend_from_slice(data);
    Ok(frame)
}

pub async fn read_assign(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<AssignRequest> {
    let mut magic = [0u8; 4];
    read_exact_async(reader, &mut magic).await?;
    if magic != MAGIC_ASSIGN {
        bail!("expected ASGN magic");
    }
    read_assign_payload(reader).await
}

pub async fn read_assign_payload(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<AssignRequest> {
    let mut hdr = [0u8; 4];
    read_exact_async(reader, &mut hdr).await?;
    let max_items = u16::from_le_bytes([hdr[0], hdr[1]]);
    let tag_len = u16::from_le_bytes([hdr[2], hdr[3]]) as usize;
    let consumer_tag = read_utf8_field(reader, tag_len, "consumer_tag").await?;
    let mut data_len_buf = [0u8; 2];
    read_exact_async(reader, &mut data_len_buf).await?;
    let data_len = u16::from_le_bytes(data_len_buf) as usize;
    let data_addr = read_utf8_field(reader, data_len, "data_addr").await?;
    if data_addr.is_empty() {
        bail!("assign data_addr empty");
    }
    Ok(AssignRequest {
        max_items,
        consumer_tag,
        data_addr,
    })
}

pub fn encode_producer_ready() -> Vec<u8> {
    MAGIC_PRODUCER_READY.to_vec()
}

pub fn encode_heartbeat(report: &ProducerHeartbeat) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + 16 + 8);
    frame.extend_from_slice(&MAGIC_HEARTBEAT);
    frame.extend_from_slice(&report.pending_messages.to_le_bytes());
    frame.extend_from_slice(&report.pending_bytes.to_le_bytes());
    frame.extend_from_slice(&report.ring_messages.to_le_bytes());
    frame.extend_from_slice(&report.ring_bytes.to_le_bytes());
    frame.extend_from_slice(&report.drops_total.to_le_bytes());
    frame
}

pub async fn read_heartbeat(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<ProducerHeartbeat> {
    let mut magic = [0u8; 4];
    read_exact_async(reader, &mut magic).await?;
    if magic != MAGIC_HEARTBEAT {
        bail!("expected HBRP magic");
    }
    read_heartbeat_payload(reader).await
}

pub async fn read_heartbeat_payload(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<ProducerHeartbeat> {
    let mut body = [0u8; 24];
    read_exact_async(reader, &mut body).await?;
    Ok(ProducerHeartbeat {
        pending_messages: u32::from_le_bytes([body[0], body[1], body[2], body[3]]),
        pending_bytes: u32::from_le_bytes([body[4], body[5], body[6], body[7]]),
        ring_messages: u32::from_le_bytes([body[8], body[9], body[10], body[11]]),
        ring_bytes: u32::from_le_bytes([body[12], body[13], body[14], body[15]]),
        drops_total: u64::from_le_bytes([
            body[16], body[17], body[18], body[19], body[20], body[21], body[22], body[23],
        ]),
    })
}

pub async fn read_producer_ready(reader: &mut (impl tokio::io::AsyncRead + Unpin)) -> Result<()> {
    let mut magic = [0u8; 4];
    read_exact_async(reader, &mut magic).await?;
    if magic != MAGIC_PRODUCER_READY {
        bail!("expected PRDY magic");
    }
    Ok(())
}

pub fn encode_delivered(report: &DeliveryReport) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + 2 + 4);
    frame.extend_from_slice(&MAGIC_DELIVERED);
    frame.extend_from_slice(&report.msg_count.to_le_bytes());
    frame.extend_from_slice(&report.batch_bytes.to_le_bytes());
    frame
}

pub async fn read_delivery_report_payload(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<DeliveryReport> {
    let mut body = [0u8; 6];
    read_exact_async(reader, &mut body).await?;
    Ok(DeliveryReport {
        msg_count: u16::from_le_bytes([body[0], body[1]]),
        batch_bytes: u32::from_le_bytes([body[2], body[3], body[4], body[5]]),
    })
}

pub async fn read_delivered(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<DeliveryReport> {
    let mut magic = [0u8; 4];
    read_exact_async(reader, &mut magic).await?;
    if magic != MAGIC_DELIVERED {
        bail!("expected DELV magic");
    }
    read_delivery_report_payload(reader).await
}

pub fn encode_failed(report: &FailureReport) -> Vec<u8> {
    let msg = report.message.as_bytes();
    let msg_len = msg.len().min(u16::MAX as usize) as u16;
    let mut frame = Vec::with_capacity(4 + 2 + 4 + 2 + 2 + msg_len as usize);
    frame.extend_from_slice(&MAGIC_FAILED);
    frame.extend_from_slice(&report.msg_count.to_le_bytes());
    frame.extend_from_slice(&report.batch_bytes.to_le_bytes());
    frame.extend_from_slice(&report.code.to_le_bytes());
    frame.extend_from_slice(&msg_len.to_le_bytes());
    frame.extend_from_slice(&msg[..msg_len as usize]);
    frame
}

pub async fn read_failure_report_payload(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<FailureReport> {
    let mut hdr = [0u8; 10];
    read_exact_async(reader, &mut hdr).await?;
    let msg_count = u16::from_le_bytes([hdr[0], hdr[1]]);
    let batch_bytes = u32::from_le_bytes([hdr[2], hdr[3], hdr[4], hdr[5]]);
    let code = u16::from_le_bytes([hdr[6], hdr[7]]);
    let msg_len = u16::from_le_bytes([hdr[8], hdr[9]]) as usize;
    let message = read_utf8_field(reader, msg_len, "fail_message").await?;
    Ok(FailureReport {
        msg_count,
        batch_bytes,
        code,
        message,
    })
}

pub async fn read_failed(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<FailureReport> {
    let mut magic = [0u8; 4];
    read_exact_async(reader, &mut magic).await?;
    if magic != MAGIC_FAILED {
        bail!("expected FAIL magic");
    }
    read_failure_report_payload(reader).await
}

/// TCP baixa latência + keepalive — detecta master morto sem esperar batch_timeout.
pub fn tune_tcp(stream: &tokio::net::TcpStream) {
    let _ = stream.set_nodelay(true);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(5))
        .with_interval(std::time::Duration::from_secs(2));
    let _ = socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_batch_roundtrip_count() {
        let items = vec![vec![1, 2, 3], vec![4, 5]];
        let frame = encode_batch(&items);
        assert_eq!(&frame[..4], MAGIC_BATCH);
        let count = u16::from_le_bytes([frame[4], frame[5]]);
        assert_eq!(count, 2);
        let plen = u32::from_le_bytes([frame[6], frame[7], frame[8], frame[9]]) as usize;
        let payload = &frame[10..10 + plen];
        assert_eq!(batch_item_count(payload).unwrap(), 2);
        assert!(payload.len() > 2 + 4 + 4 + 4);
    }

    #[test]
    fn parse_ready_with_tag() {
        let mut body = Vec::from(MAGIC_CONSUMER_READY);
        body.extend_from_slice(&32u16.to_le_bytes());
        body.extend_from_slice(&11u16.to_le_bytes());
        body.extend_from_slice(b"consumer-g0");
        let req = parse_ready(&body).unwrap();
        assert_eq!(req.max_items, 32);
        assert_eq!(req.consumer_tag, "consumer-g0");
    }

    #[test]
    fn parse_ready_empty_tag() {
        let mut body = Vec::from(MAGIC_CONSUMER_READY);
        body.extend_from_slice(&16u16.to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());
        let req = parse_ready(&body).unwrap();
        assert!(req.consumer_tag.is_empty());
    }

    #[test]
    fn pub_batch_roundtrip() {
        let items = vec![vec![1, 2, 3], vec![4, 5, 6, 7]];
        let frame = encode_pub_batch(&items);
        assert_eq!(&frame[..4], MAGIC_PUB_BATCH);
        let plen = u32::from_le_bytes([frame[6], frame[7], frame[8], frame[9]]) as usize;
        let payload = Bytes::copy_from_slice(&frame[10..10 + plen]);
        let parsed = parse_batch_payload_items(payload).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].as_ref(), &[1, 2, 3]);
        assert_eq!(parsed[1].as_ref(), &[4, 5, 6, 7]);
    }

    #[test]
    fn pub_batch_slices_share_parent() {
        let items = vec![vec![1, 2, 3], vec![4, 5]];
        let frame = encode_pub_batch(&items);
        let plen = u32::from_le_bytes([frame[6], frame[7], frame[8], frame[9]]) as usize;
        let payload = Bytes::copy_from_slice(&frame[10..10 + plen]);
        let base = payload.as_ptr();
        let parsed = parse_batch_payload_items(payload).unwrap();
        for item in &parsed {
            let start = item.as_ptr() as usize - base as usize;
            assert!(start + item.len() <= plen);
        }
    }

    #[test]
    fn assign_roundtrip() {
        let req = AssignRequest {
            max_items: 32,
            consumer_tag: "consumer-g0".into(),
            data_addr: "127.0.0.1:9760".into(),
        };
        let frame = encode_assign(&req).unwrap();
        assert_eq!(&frame[..4], MAGIC_ASSIGN);
    }

    #[test]
    fn register_roundtrip() {
        let frame = encode_register("127.0.0.1:9760").unwrap();
        assert_eq!(&frame[..4], MAGIC_REGISTER);
    }

    #[test]
    fn heartbeat_payload_size() {
        let hb = ProducerHeartbeat {
            pending_messages: 10,
            pending_bytes: 1024,
            ring_messages: 5,
            ring_bytes: 512,
            drops_total: 3,
        };
        let frame = encode_heartbeat(&hb);
        assert_eq!(frame.len(), 4 + 24);
        assert_eq!(&frame[..4], MAGIC_HEARTBEAT);
    }
}
