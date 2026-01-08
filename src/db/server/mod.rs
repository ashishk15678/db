// Unified Server - Supports both raw TCP protocol and HTTP
// Auto-detects protocol based on first bytes of connection

use std::io::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::db::executor::ExecutionResult;
use crate::db::sql::execute_sql;

/// Message types for raw TCP protocol
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

/// Binary message for TCP protocol: [length: 4 bytes LE][type: 1 byte][payload]
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

    pub fn pong() -> Self {
        Self {
            msg_type: MessageType::Pong,
            payload: vec![],
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let len = self.payload.len() as u32;
        let mut bytes = Vec::with_capacity(5 + self.payload.len());
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.push(self.msg_type as u8);
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    pub async fn read_async(stream: &mut TcpStream) -> std::io::Result<Self> {
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

    pub async fn write_async(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        stream.write_all(&self.to_bytes()).await
    }

    pub fn payload_str(&self) -> String {
        String::from_utf8_lossy(&self.payload).to_string()
    }
}

/// Handle raw TCP protocol connection
async fn handle_tcp_protocol(mut stream: TcpStream) {
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

/// Handle HTTP protocol connection  
async fn handle_http_protocol(mut stream: TcpStream, initial_data: &[u8]) {
    // Read rest of HTTP request
    let mut buffer = vec![0u8; 8192];
    buffer[..initial_data.len()].copy_from_slice(initial_data);
    
    let bytes_read = match stream.read(&mut buffer[initial_data.len()..]).await {
        Ok(n) => initial_data.len() + n,
        Err(_) => initial_data.len(),
    };

    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
    let first_line = request.lines().next().unwrap_or("");
    
    let response = if let Some((method, path)) = parse_http_request_line(first_line) {
        match (method, path) {
            (_, "/ping") => http_response(200, "text/plain", "pong\n"),
            (_, "/heart-beat") => http_response(200, "text/plain", "OK\n"),
            ("POST", "/sql") | ("GET", "/sql") => {
                let body = extract_http_body(&request);
                if body.is_empty() {
                    http_response(400, "application/json", r#"{"error":"No SQL query provided"}"#)
                } else {
                    let result = execute_sql(&body);
                    let status = match &result {
                        ExecutionResult::Error { .. } => 400,
                        _ => 200,
                    };
                    http_response(status, "application/json", &result.to_json())
                }
            }
            (_, "/tables") => {
                match crate::db::catalog::CATALOG.list_tables() {
                    Ok(tables) => {
                        let json = serde_json::json!({"tables": tables});
                        http_response(200, "application/json", &json.to_string())
                    }
                    Err(e) => http_response(400, "application/json", &format!(r#"{{"error":"{}"}}"#, e)),
                }
            }
            _ => http_response(404, "application/json", r#"{"error":"Not Found"}"#),
        }
    } else {
        http_response(400, "application/json", r#"{"error":"Bad Request"}"#)
    };

    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

fn http_response(status: u16, content_type: &str, body: &str) -> String {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Error",
    };
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status, status_text, content_type, body.len(), body
    )
}

fn parse_http_request_line(line: &str) -> Option<(&str, &str)> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 {
        Some((parts[0], parts[1]))
    } else {
        None
    }
}

fn extract_http_body(request: &str) -> String {
    if let Some(pos) = request.find("\r\n\r\n") {
        request[pos + 4..].trim_matches('\0').trim().to_string()
    } else if let Some(pos) = request.find("\n\n") {
        request[pos + 2..].trim_matches('\0').trim().to_string()
    } else {
        String::new()
    }
}

/// Start unified server that handles both TCP and HTTP
pub async fn start_server(addr: &str) -> Result<(), Error> {
    let listener = TcpListener::bind(addr).await?;
    println!("🦋 butterfly_db listening on {} (TCP + HTTP)", addr);

    loop {
        let (mut stream, peer_addr) = listener.accept().await?;
        
        tokio::spawn(async move {
            // Peek at first bytes to detect protocol
            let mut peek_buf = [0u8; 5];
            match stream.peek(&mut peek_buf).await {
                Ok(n) if n >= 4 => {
                    // Check if it looks like HTTP (starts with GET, POST, PUT, etc.)
                    let start = String::from_utf8_lossy(&peek_buf);
                    if start.starts_with("GET ") || start.starts_with("POST") || 
                       start.starts_with("PUT ") || start.starts_with("HEAD") ||
                       start.starts_with("DELE") || start.starts_with("OPTI") {
                        // HTTP protocol
                        let mut initial = vec![0u8; n];
                        let _ = stream.read(&mut initial).await;
                        handle_http_protocol(stream, &initial).await;
                    } else {
                        // Raw TCP protocol  
                        handle_tcp_protocol(stream).await;
                    }
                }
                _ => {
                    // Too few bytes, assume HTTP
                    handle_http_protocol(stream, &[]).await;
                }
            }
        });
    }
}

/// TCP client for raw protocol (faster than HTTP)
pub struct DbClient {
    stream: TcpStream,
}

impl DbClient {
    /// Connect to database server
    pub async fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    /// Execute a SQL query
    pub async fn query(&mut self, sql: &str) -> std::io::Result<String> {
        let msg = Message::query(sql);
        msg.write_async(&mut self.stream).await?;
        
        let response = Message::read_async(&mut self.stream).await?;
        Ok(response.payload_str())
    }

    /// Ping the server
    pub async fn ping(&mut self) -> std::io::Result<bool> {
        let msg = Message {
            msg_type: MessageType::Ping,
            payload: vec![],
        };
        msg.write_async(&mut self.stream).await?;
        
        let response = Message::read_async(&mut self.stream).await?;
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
        
        assert_eq!(bytes[0..4], (19u32).to_le_bytes());
        assert_eq!(bytes[4], MessageType::Query as u8);
        assert_eq!(&bytes[5..], b"SELECT * FROM users");
    }
}
