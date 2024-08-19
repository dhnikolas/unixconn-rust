
use std::os::unix::net::UnixStream;
use std::io::{self, Read, Write, BufReader};
use std::error::Error;
use bytes::{Bytes, BytesMut, BufMut};
use uuid::Uuid;
use std::time::Duration;

const PROTOCOL_FIELDS: usize = 4;

fn metadata_delim() -> &'static [u8] {
    &[0x1E]
}

fn message_delim() -> u8 {
    0x1F
}

struct Message {
    request_id: String,
    method_name: String,
    body: Bytes,
    error: String,
}

pub struct Client {
    conn: UnixStream,
    timeout: u64
}

impl Client {
    pub fn new(address: &str, timeout: u64) -> Result<Self, Box<dyn Error>> {
        let conn = UnixStream::connect(address)?;
        Ok(Client { conn, timeout })
    }

    pub fn close(&self) -> io::Result<()> {
        self.conn.shutdown(std::net::Shutdown::Both)
    }

    pub fn do_request(&mut self, method_name: &str, request_body: &[u8]) -> Result<Bytes, Box<dyn Error>> {
        let request_id = Uuid::new_v4().to_string();
        let request = Message {
            request_id: request_id.clone(),
            method_name: method_name.to_string(),
            body: Bytes::from(request_body.to_vec()),
            error: String::new(),
        };

        let raw_request = message_to_bytes(&request);
        self.conn.write_all(&raw_request)?;
        self.conn.set_read_timeout(Some(Duration::from_secs(self.timeout)))?;

        let mut reader = BufReader::new(&self.conn);
        let message = read_message(&mut reader)?;

        if !message.error.is_empty() {
            return Err(format!("client response error: {}", message.error).into());
        }

        if message.request_id != request_id {
            return Err(format!("client wrong requestID error: {}", message.error).into());
        }

        Ok(message.body)
    }
}

fn parse_message(body: &[u8]) -> Result<Message, Box<dyn Error>> {
    let parts: Vec<&[u8]> = body.split(|&b| b == metadata_delim()[0]).collect();
    if parts.len() != PROTOCOL_FIELDS {
        return Err(format!("error protocol received message with {} parts, expected {}", parts.len(), PROTOCOL_FIELDS).into());
    }

    Ok(Message {
        request_id: String::from_utf8(parts[0].to_vec())?,
        method_name: String::from_utf8(parts[1].to_vec())?,
        error: String::from_utf8(parts[2].to_vec())?,
        body: Bytes::from(parts[3].to_vec()),
    })
}

fn message_to_bytes(r: &Message) -> Bytes {
    let mut buffer = BytesMut::new();

    buffer.put(r.request_id.as_bytes());
    buffer.put(metadata_delim());

    buffer.put(r.method_name.as_bytes());
    buffer.put(metadata_delim());

    buffer.put(r.error.as_bytes());
    buffer.put(metadata_delim());

    buffer.put(&r.body[..]);
    buffer.put_u8(message_delim());

    buffer.freeze()
}

fn read_message<R: Read>(reader: &mut R) -> Result<Message, Box<dyn Error>> {
    let mut message_body = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        reader.read_exact(&mut byte)?;
        if byte[0] == message_delim() {
            break;
        }
        message_body.push(byte[0]);
    }

    parse_message(&message_body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_client() -> Result<(), Box<dyn Error>> {
        let mut client = Client::new("/tmp/salt-ssd.sock", 10)?;
        let method_name = "getnssusers";
        let request_body = b"";

        let response = client.do_request(method_name, request_body)?;
        match std::str::from_utf8(&response) {
            Ok(s) => println!("Received response: {}", s),
            Err(e) => eprintln!("Response was not valid UTF-8: {}", e),
        }
        client.close()?;

        Ok(())
    }

    #[test]
    fn it_works() {
        if let Err(e) = run_client() {
            eprintln!("Error: {}", e.to_string());
            std::process::exit(1);
        }
    }
}
