#[macro_export]
macro_rules! info {
    ($($expr:expr),*) => {
        use colored::Colorize;
        use chrono::Local;

        let now = Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();

        println!("{} {} {}", timestamp.bold(), "[INFO]  : ".cyan(), format!("{:?}", $($expr),*).cyan());

    };
}

#[macro_export]
macro_rules! error {
    ($($expr:expr),*) => {
        // Get the current local time and format it
        let now = Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();

        println!("{} {} {}", timestamp.bold() , "[ERROR] : ".red(), format!("{:?}", $($expr),*).red());

    };
}

#[macro_export]
macro_rules! warn {
    ($($expr:expr),*) => {
        // Get the current local time and format it
        let now = Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();

        println!("{} {} {}", timestamp.bold(), "[WARN]  : ".yellow(), format!("{:?}", $($expr),*).yellow());

    };
}

pub fn debug(string: &str) -> () {
    use chrono::Local;
    use colored::Colorize;

    // Get the current local time and format it
    let now = Local::now();
    let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string().bold();

    println!("{timestamp} [DEBUG] : {:?}", string);
}
