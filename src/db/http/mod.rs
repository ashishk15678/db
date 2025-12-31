use std::{
    fmt::Display,
    io::{Error, Read, Write},
    net::TcpStream,
};

use crate::db::sql::execute_sql;
use crate::db::executor::ExecutionResult;

#[derive(Debug)]
pub struct HttpResponse {
    pub status_code: usize,
    pub protocol: String,
    pub headers: String,
    pub body: String,
}

impl Display for HttpResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}\r\n{}\r\n\r\n{}",
            self.protocol, self.status_code, self.headers, self.body
        )
    }
}

impl HttpResponse {
    fn json(status_code: usize, body: String) -> Self {
        HttpResponse {
            status_code,
            protocol: "HTTP/1.1".to_string(),
            headers: format!(
                "Content-Type: application/json\r\nContent-Length: {}",
                body.len()
            ),
            body,
        }
    }

    fn text(status_code: usize, body: String) -> Self {
        HttpResponse {
            status_code,
            protocol: "HTTP/1.1".to_string(),
            headers: format!(
                "Content-Type: text/plain\r\nContent-Length: {}",
                body.len()
            ),
            body,
        }
    }
}

/// Extract HTTP request body from raw request
fn extract_body(request: &str) -> String {
    // Find the blank line that separates headers from body
    if let Some(pos) = request.find("\r\n\r\n") {
        request[pos + 4..].trim_matches('\0').trim().to_string()
    } else if let Some(pos) = request.find("\n\n") {
        request[pos + 2..].trim_matches('\0').trim().to_string()
    } else {
        String::new()
    }
}

pub async fn handle_client(mut stream: TcpStream) -> Result<(), Error> {
    let mut buffer = [0; 8192]; // Larger buffer for SQL queries
    let bytes_read = stream.read(&mut buffer)?;
    
    let request_as_str = String::from_utf8_lossy(&buffer[..bytes_read]);
    let first_request_line = request_as_str.lines().next().unwrap_or("");

    let response = match parse_request_line(first_request_line) {
        Some((method, path)) => {
            match (method, path) {
                (_, "/heart-beat") => {
                    HttpResponse::text(200, "OK\n".to_string())
                }
                (_, "/ping") => {
                    HttpResponse::text(200, "pong\n".to_string())
                }
                ("POST", "/sql") | ("GET", "/sql") => {
                    // Extract SQL from request body
                    let sql = extract_body(&request_as_str);
                    
                    if sql.is_empty() {
                        HttpResponse::json(400, r#"{"error":"No SQL query provided. Send SQL in request body."}"#.to_string())
                    } else {
                        // Execute the SQL query
                        let result = execute_sql(&sql);
                        let status = match &result {
                            ExecutionResult::Error { .. } => 400,
                            _ => 200,
                        };
                        HttpResponse::json(status, result.to_json())
                    }
                }
                (_, "/tables") => {
                    // List all tables
                    match crate::db::catalog::CATALOG.list_tables() {
                        Ok(tables) => {
                            let json = serde_json::json!({
                                "tables": tables
                            });
                            HttpResponse::json(200, json.to_string())
                        }
                        Err(e) => {
                            HttpResponse::json(400, format!(r#"{{"error":"{}"}}"#, e))
                        }
                    }
                }
                _ => {
                    HttpResponse::json(404, r#"{"error":"Not Found"}"#.to_string())
                }
            }
        }
        None => {
            HttpResponse::json(400, r#"{"error":"Bad Request"}"#.to_string())
        }
    };

    stream.write_all(response.to_string().as_bytes())?;
    stream.flush()?;
    println!("Responded with {} to {}", response.status_code, first_request_line);
    Ok(())
}

/// Parse HTTP request line and return (method, path)
pub fn parse_request_line(request_line: &str) -> Option<(&str, &str)> {
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() >= 2 {
        Some((parts[0], parts[1]))
    } else {
        None
    }
}

// Keep old function name as alias for backward compatibility
pub async fn handleClient(stream: TcpStream) -> Result<(), Error> {
    handle_client(stream).await
}
