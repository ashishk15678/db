use protobuf::text_format::print_to_string;

pub struct SQL;

impl SQL {
    pub fn parse(mut query: String) -> String {
        query = query.trim().to_string();
        let parts: Vec<&str> = query.split_whitespace().collect();
        match parts[0].to_lowercase().as_str() {
            "create" => match parts[1] {
                "table" => SQL::create_table(parts),
                _ => String::from("Error: Missing or invalid subcommand for 'create'"),
            },
            "select" => SQL::select_query(parts),
            "insert" => SQL::insert_query(parts),
            "update" => SQL::update_query(parts),
            "delete" => SQL::delete_query(parts),
            _ => String::from("Error: Unknown command"),
        }
    }

    pub fn create_table(parts: Vec<&str>) -> String {
        String::from("ok")
    }

    pub fn select_query(parts: Vec<&str>) -> String {
        "".to_string()
    }

    pub fn insert_query(parts: Vec<&str>) -> String {
        "".to_string()
    }

    pub fn update_query(parts: Vec<&str>) -> String {
        "".to_string()
    }

    pub fn delete_query(parts: Vec<&str>) -> String {
        "".to_string()
    }
}
