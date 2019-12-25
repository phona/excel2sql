#[macro_use] 
extern crate log;
extern crate mysql;

mod utils;

use utils::Opts;
use structopt::StructOpt;

fn main() {
    let opts = Opts::from_args();
    println!("{:?}", opts)
}