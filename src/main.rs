#[macro_use] 
extern crate log;
extern crate mysql;

mod utils;
mod error;

use utils::parse;

fn main() {
    parse();
}