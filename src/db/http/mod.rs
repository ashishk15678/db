use std::{
    fmt::Display,
    io::{Error, Read, Write},
    net::TcpStream,
};

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

pub fn handleClient(mut stream: TcpStream) -> Result<(), Error> {
    let mut buffer = [0; 1024];
    stream.read(&mut buffer).unwrap();

    let request_as_str = String::from_utf8_lossy(&buffer);
    let first_request_line = request_as_str.lines().next().unwrap_or("");

    let response = match parse_request_line(first_request_line) {
        Some(path) => {
            match path {
                "/heart-beat" => {
                    let body = "OK\n";
                    HttpResponse {
                        status_code: 200,
                        protocol: "HTTP/1.1".to_string(),
                        headers: format!(
                            "Content-Type: text/plain\r\nContent-Length: {}",
                            body.len()
                        ),
                        body: body.to_string(),
                    }
                }
                "/ping" => {
                    let body = "pong\n";
                    HttpResponse {
                        status_code: 200,
                        protocol: "HTTP/1.1".to_string(),
                        headers: format!(
                            "Content-Type: text/plain\r\nContent-Length: {}",
                            body.len()
                        ),
                        body: body.to_string(),
                    }
                }
                "/sql" => {
                    let body = "pong\n";
                    HttpResponse {
                        status_code: 200,
                        protocol: "HTTP/1.1".to_string(),
                        headers: format!(
                            "Content-Type: text/plain\r\nContent-Length: {}",
                            body.len()
                        ),
                        body: body.to_string(),
                    }
                }
                _ => {
                    // All other valid paths
                    let body = "404 Not Found\n";
                    HttpResponse {
                        status_code: 404,
                        protocol: "HTTP/1.1".to_string(),
                        headers: format!(
                            "Content-Type: text/plain\r\nContent-Length: {}",
                            body.len()
                        ),
                        body: body.to_string(),
                    }
                }
            }
        }
        None => {
            // Catches the case where the request line is malformed
            let body = "400 Bad Request\n";
            HttpResponse {
                status_code: 400,
                protocol: "HTTP/1.1".to_string(),
                headers: format!("Content-Type: text/plain\r\nContent-Length: {}", body.len()),
                body: body.to_string(),
            }
        }
    };

    stream.write_all(response.to_string().as_bytes())?;
    stream.flush()?;
    println!("Responded with {}", response.status_code);
    Ok(())
}

pub fn parse_request_line(request_line: &str) -> Option<&str> {
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() >= 2 {
        // The path is typically the second element (index 1) in the split parts.
        Some(parts[1])
    } else {
        // If there aren't enough parts, the request line is malformed.
        None
    }
}
