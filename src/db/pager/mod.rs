#![allow(non_snake_case)]
#[allow(unused_variables)]
/////////////////////////////////////////////
/// CONSTANTS ///////////////////////////////
/////////////////////////////////////////////

pub const MAX_FILE_SIZE: usize = 4 * 1024 * 1024;

////////////////////////////////////////////
/// STRUCTS ////////////////////////////////
////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Record {
    recordId: String,
    data: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct Page {
    pageId: String,
    records: Vec<Record>,
}

trait DbCommon {
    fn new() -> Self;
    fn get_id(&self) -> String;
}

impl DbCommon for Record {
    fn new() -> Record {
        Record {
            recordId: String::from("random-id-uuid"),
            data: vec![],
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
        }
    }

    fn get_id(&self) -> String {
        self.pageId.clone()
    }
}
