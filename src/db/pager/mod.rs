#![allow(non_snake_case)]

use std::io::{Cursor, Read, Write};
#[allow(unused_variables)]
/////////////////////////////////////////////
/// CONSTANTS ///////////////////////////////
/////////////////////////////////////////////

pub const MAX_FILE_SIZE: usize = 4 * 1024 * 1024;
pub const PAGE_SIZE: usize = 4 * 1024;
////////////////////////////////////////////
/// STRUCTS ////////////////////////////////
////////////////////////////////////////////

impl TryFrom<u8> for RecordStatus {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RecordStatus::Active),
            1 => Ok(RecordStatus::Deleted),
            2 => Ok(RecordStatus::Archived),
            _ => Err("Invalid Recordstatus "),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone)]
enum RecordStatus {
    Active = 0,
    Deleted = 1,
    Archived = 2,
}

pub struct Record {
    recordId: String,
    data: Vec<u8>,
    pageId: Option<String>,
    status: RecordStatus,
}

pub struct Page {
    pageId: String,
    records: Vec<Record>,
    data: Vec<u8>,
}

impl Page {
    pub fn read_data(&self, offset: usize, buffer: &mut [u8]) -> Result<(), std::io::Error> {
        let mut cursor = Cursor::new(&self.data[offset..]);
        cursor.read_exact(buffer)
    }

    pub fn write_data(&mut self, offset: usize, buffer: &mut [u8]) -> Result<(), std::io::Error> {
        let mut cursor = Cursor::new(&mut self.data[offset..]);
        cursor.write_all(buffer)
    }
}

impl Record {
    pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
        let mut cursor = Cursor::new(bytes);

        // Read the fixed-size header first
        let mut status_byte = [0; 1];
        let mut id_bytes = [0; 2];
        let mut data_len_bytes = [0; 4];

        cursor.read_exact(&mut status_byte)?;
        cursor.read_exact(&mut id_bytes)?;
        cursor.read_exact(&mut data_len_bytes)?;

        let status = RecordStatus::try_from(status_byte[0])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let id = u16::from_le_bytes(id_bytes);
        let data_len = u32::from_le_bytes(data_len_bytes) as usize;

        // Read the remaining data payload
        let mut data = vec![0; data_len];
        cursor.read_exact(&mut data)?;

        Ok(Record {
            recordId: id.to_string(),
            pageId: Some(status_byte[0].to_string()),
            data: data,
            status,
        })
    }

    /// Serializes a Record into a byte vector for storage.
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write the header
        // Every page will have a small header at the top
        // this header will define how the page handles everything

        bytes.push(self.status.clone() as u8);
        bytes.extend_from_slice(&self.recordId.clone().into_bytes());
        bytes.extend_from_slice(&(self.data.len() as u32).to_le_bytes());

        // Write the payload
        bytes.extend_from_slice(&self.data);
        bytes
    }

    pub fn getSize(&self) -> usize {
        self.serialize().len()
    }
}

impl Read for Record {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }
}

trait DbCommon {
    fn new() -> Self;
    fn get_id(&self) -> String;
}

impl DbCommon for Record {
    fn new() -> Record {
        Record {
            pageId: None,
            recordId: String::from("random-id-uuid"),
            data: vec![],
            status: RecordStatus::Active,
        }
    }

    fn get_id(&self) -> String {
        self.recordId.clone()
    }
}

impl DbCommon for Page {
    fn new() -> Self {
        Page {
            pageId: String::from("some-random-uuid"),
            records: vec![],
            data: vec![0; PAGE_SIZE],
        }
    }

    fn get_id(&self) -> String {
        self.pageId.clone()
    }
}
