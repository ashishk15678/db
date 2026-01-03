use std::io::{Read, Write};
use std::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener as TokioTcpListener;

use crate::db::sql::execute_sql;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum MessageType {
    Query = 1,
    Result = 2,
    Error = 3,
    Ping = 4,
    Pong = 5,
}

impl From<u8> for MessageType {
    fn from(v: u8) -> Self {
        match v {
            1 => MessageType::Query,
            2 => MessageType::Result,
            3 => MessageType::Error,
            4 => MessageType::Ping,
            5 => MessageType::Pong,
            _ => MessageType::Error,
        }
    }
}

/// [length: 4 bytes LE][type: 1 byte][payload: length bytes]
pub struct Message {
    pub msg_type: MessageType,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn query(sql: &str) -> Self {
        Self {
            msg_type: MessageType::Query,
            payload: sql.as_bytes().to_vec(),
        }
    }

    pub fn result(data: &str) -> Self {
        Self {
            msg_type: MessageType::Result,
            payload: data.as_bytes().to_vec(),
        }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            msg_type: MessageType::Error,
            payload: msg.as_bytes().to_vec(),
        }
    }

    pub fn ping() -> Self {
        Self {
            msg_type: MessageType::Ping,
            payload: vec![],
        }
    }

    pub fn pong() -> Self {
        Self {
            msg_type: MessageType::Pong,
            payload: vec![],
        }
    }

    /// Serialize message to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let len = self.payload.len() as u32;
        let mut bytes = Vec::with_capacity(5 + self.payload.len());
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.push(self.msg_type as u8);
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    /// Read message from stream (sync)
    pub fn read_from(stream: &mut TcpStream) -> std::io::Result<Self> {
        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut type_byte = [0u8; 1];
        stream.read_exact(&mut type_byte)?;
        let msg_type = MessageType::from(type_byte[0]);

        let mut payload = vec![0u8; len];
        if len > 0 {
            stream.read_exact(&mut payload)?;
        }

        Ok(Self { msg_type, payload })
    }

    /// Write message to stream (sync)
    pub fn write_to(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        stream.write_all(&self.to_bytes())
    }

    /// Read message from async stream
    pub async fn read_async(stream: &mut tokio::net::TcpStream) -> std::io::Result<Self> {
        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut type_byte = [0u8; 1];
        stream.read_exact(&mut type_byte).await?;
        let msg_type = MessageType::from(type_byte[0]);

        let mut payload = vec![0u8; len];
        if len > 0 {
            stream.read_exact(&mut payload).await?;
        }

        Ok(Self { msg_type, payload })
    }

    /// Write message to async stream
    pub async fn write_async(&self, stream: &mut tokio::net::TcpStream) -> std::io::Result<()> {
        stream.write_all(&self.to_bytes()).await
    }

    /// Get payload as string
    pub fn payload_str(&self) -> String {
        String::from_utf8_lossy(&self.payload).to_string()
    }
}

/// Handle a TCP client connection
pub async fn handle_tcp_client(mut stream: tokio::net::TcpStream) {
    loop {
        let msg = match Message::read_async(&mut stream).await {
            Ok(m) => m,
            Err(_) => break, // Client disconnected
        };

        let response = match msg.msg_type {
            MessageType::Query => {
                let sql = msg.payload_str();
                let result = execute_sql(&sql);
                Message::result(&result.to_json())
            }
            MessageType::Ping => Message::pong(),
            _ => Message::error("Unknown command"),
        };

        if response.write_async(&mut stream).await.is_err() {
            break;
        }
    }
}

/// Start TCP server
pub async fn start_tcp_server(addr: String) -> std::io::Result<()> {
    let listener = TokioTcpListener::bind(addr).await?;

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        println!("TCP connection from: {}", peer_addr);

        tokio::spawn(async move {
            handle_tcp_client(stream).await;
        });
    }
}

pub struct TcpClient {
    stream: TcpStream,
}

impl TcpClient {
    /// Connect to database server
    pub fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Self { stream })
    }

    /// Execute a SQL query
    pub fn query(&mut self, sql: &str) -> std::io::Result<String> {
        let msg = Message::query(sql);
        msg.write_to(&mut self.stream)?;

        let response = Message::read_from(&mut self.stream)?;
        Ok(response.payload_str())
    }

    /// Ping the server
    pub fn ping(&mut self) -> std::io::Result<bool> {
        let msg = Message::ping();
        msg.write_to(&mut self.stream)?;

        let response = Message::read_from(&mut self.stream)?;
        Ok(matches!(response.msg_type, MessageType::Pong))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialization() {
        let msg = Message::query("SELECT * FROM users");
        let bytes = msg.to_bytes();

        assert_eq!(bytes[0..4], (19u32).to_le_bytes()); // length
        assert_eq!(bytes[4], MessageType::Query as u8);
        assert_eq!(&bytes[5..], b"SELECT * FROM users");
    }

    #[test]
    fn test_message_types() {
        assert!(matches!(MessageType::from(1), MessageType::Query));
        assert!(matches!(MessageType::from(2), MessageType::Result));
        assert!(matches!(MessageType::from(3), MessageType::Error));
        assert!(matches!(MessageType::from(4), MessageType::Ping));
        assert!(matches!(MessageType::from(5), MessageType::Pong));
    }
}
