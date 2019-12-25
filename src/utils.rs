use std::ffi::OsStr;
use std::iter::Skip;
use std::path::Path;

use calamine::{
    open_workbook, DataType, Error as ExcelError, Range, RangeDeserializer,
    RangeDeserializerBuilder, Reader, Xlsx,
};
use mysql::{
    params, Error as MySqlError, Opts as MySqlOpts, OptsBuilder as MysqlOptsBuilder, Pool,
};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "Migrate data from excel to database")]
pub struct Opts {
    #[structopt(short = "e", long = "excel")]
    excel: String,

    #[structopt(short = "t", long = "database-type")]
    database_type: String,

    #[structopt(short = "h", long = "host")]
    host: String,

    #[structopt(short = "p", long = "port")]
    port: u16,

    #[structopt(short = "U", long = "user")]
    user: String,

    #[structopt(short = "P", long = "password")]
    password: String,

    #[structopt(short = "c", long = "clear")]
    clear: bool,

    #[structopt(short = "s", long = "skip")]
    skip: u32,
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub fields: Vec<String>,
    range: Range<DataType>,
}

impl Table {
    pub fn new(table_name: &str, range: Range<DataType>) -> Result<Self, ExcelError> {
        let mut fields = Vec::new();
        let mut iter = RangeDeserializerBuilder::new().from_range(&range)?;

        if let Some(result) = iter.next() {
            let row: Vec<DataType> = result?;
            for i in row.iter() {
                if let DataType::String(f) = i {
                    fields.push(String::from(f));
                }
            }
        }

        Ok(Table {
            name: String::from(table_name),
            fields,
            range,
        })
    }

    pub fn iter_rows(
        &self,
        skip: usize,
    ) -> Result<Skip<RangeDeserializer<DataType, Vec<DataType>>>, ExcelError> {
        let iter = RangeDeserializerBuilder::new().from_range(&self.range)?;
        Ok(iter.skip(skip))
    }
}

pub fn parse_excel(filepath: &str) -> Result<Vec<Table>, ExcelError> {
    let mut sheets = Vec::new();
    let mut workbook: Xlsx<_> = open_workbook(&filepath)?;

    for sheet_name in workbook.sheet_names().to_owned().iter() {
        if let Some(Ok(range)) = workbook.worksheet_range(sheet_name) {
            sheets.push(Table::new(sheet_name, range)?);
        } else {
            warn!("sheet {} not found", sheet_name);
        }
    }

    Ok(sheets)
}

pub fn import_table_to_database(opts: &Opts, pool: &Pool, table: Table) -> Result<u32, MySqlError> {
    check_table_exists(&table.name, pool)?;

    Ok(1)
}

pub fn check_table_exists(table_name: &str, pool: &Pool) -> Result<(), MySqlError> {
    let result = pool.first_exec(format!("SHOW TABLES LIKE \"{}\"", table_name), ())?;

    if result.is_none() {
        Err(MySqlError::from(mysql::MySqlError {
            state: String::from("-1"),
            message: format!("Table '{}' doesn't exist", table_name),
            code: 99,
        }))
    } else {
        Ok(())
    }
}

pub fn make_django_style_table_name(filepath: &str, table: &Table) -> String {
    let file_stem = Path::new(filepath)
        .file_stem()
        .or(Some(OsStr::new("")))
        .and_then(|os_str| os_str.to_str())
        .unwrap_or("");

    let mut result = String::new();
    result.push_str(file_stem);
    result.push_str("_");
    result.push_str(table.name.to_lowercase().as_str());
    result
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_excel() {
        let result = parse_excel("manifest/main.xlsx");
        if let Ok(r) = result {
            assert_eq!(r.len(), 8);

            if let Ok(_rows) = &r[0].iter_rows(1) {
            } else {
                panic!("Invalid first table")
            }
        } else {
            panic!("{}", result.unwrap_err())
        }
    }

    #[test]
    fn test_make_django_style_table_name_v1() {
        let filepath = "/root/developenv/rustlang/excel2sql/manifest/main.xlsx";

        if let Ok(table) = Table::new("Video", Range::empty()) {
            let result = make_django_style_table_name(filepath, &table);
            assert_eq!(result, "main_video")
        } else {
            panic!("Invalid table")
        }
    }

    #[test]
    fn test_make_django_style_table_name_v2() {
        let filepath = "/root/developenv/rustlang/excel2sql/manifest/platform.xlsx";

        if let Ok(table) = Table::new("KeyValue", Range::empty()) {
            let result = make_django_style_table_name(filepath, &table);
            assert_eq!(result, "platform_keyvalue")
        } else {
            panic!("Invalid table")
        }
    }

    fn check<T: std::fmt::Debug, E: std::error::Error>(res: Result<T, E>) {
        match res {
            Ok(val) => println!("{:?}", val),
            Err(e) => {
                eprintln!("{:?}", e);
                assert!(false, String::from(e.description()))
            }
        }
    }

    #[test]
    fn test_check_table_exists_v1() {
        let mut builder = MysqlOptsBuilder::new();
        builder
            .db_name(Some("UBOX_english_hn_lt"))
            .ip_or_hostname(Some("localhost"))
            .user(Some("root"))
            .pass(Some("123456"));

        let pool = Pool::new(MySqlOpts::from(builder)).unwrap();

        assert!(check_table_exists("main_video", &pool).is_ok());
        assert!(check_table_exists("haha", &pool).is_err());
    }
}
