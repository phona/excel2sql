use std::ffi::OsStr;
use std::iter::Skip;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use crate::error::{CalaError, Error, MySQLError};
use calamine::{
    open_workbook, DataType, Range, RangeDeserializer, RangeDeserializerBuilder, Reader, Xlsx,
};
use mysql::Pool;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "Migrate data from excel to database")]
pub struct Opts {
    #[structopt(short = "e", long = "excel")]
    excel: String,

    #[structopt(short = "d", long = "database")]
    database: String,

    #[structopt(short = "t", long = "database-type", default_value = "mysql")]
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

    #[structopt(short = "s", long = "skip", default_value = "0")]
    skip: usize,

    #[structopt(short = "D", long = "django-style")]
    django_style: bool,
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub fields: Vec<String>,
    range: Range<DataType>,
}

impl Table {
    pub fn new(table_name: &str, range: Range<DataType>) -> Result<Self, CalaError> {
        let mut fields = Vec::new();
        let mut iter = RangeDeserializerBuilder::new()
            .has_headers(false)
            .from_range(&range)?;

        if let Some(result) = iter.next() {
            let row: Vec<DataType> = result?;
            for i in row.iter() {
                if let DataType::String(f) = i {
                    fields.push(String::from(f));
                }
            }
        }

        let mut table_name_result = String::new();
        for i in table_name.chars() {
            if i.is_ascii() && i != '(' && i != ')' {
                table_name_result.push(i);
            }
        }

        Ok(Table {
            name: table_name_result,
            fields,
            range,
        })
    }

    pub fn iter_rows(
        &self,
        skip: usize,
    ) -> Result<Skip<RangeDeserializer<DataType, Vec<DataType>>>, CalaError> {
        let iter = RangeDeserializerBuilder::new().from_range(&self.range)?;
        Ok(iter.skip(skip))
    }

    pub fn to_django_style_fields(&mut self) {
        for i in 0..self.fields.len() {
            if self.fields[i] != "id" {
                self.fields[i] = format!("c_{}", self.fields[i]);
            }
        }
    }
}

pub fn parse_excel(filepath: &str) -> Result<Vec<Table>, CalaError> {
    let mut tables = Vec::new();
    let mut workbook: Xlsx<_> = open_workbook(&filepath)?;

    for sheet_name in workbook.sheet_names().to_owned().iter() {
        if let Some(Ok(range)) = workbook.worksheet_range(sheet_name) {
            tables.push(Table::new(sheet_name, range)?);
        } else {
            warn!("sheet {} not found", sheet_name);
        }
    }

    Ok(tables)
}

pub fn import_table_to_database(
    opts: Arc<Opts>,
    pool: Arc<Pool>,
    table: Table,
) -> Result<(u32, String), Error> {
    let table_name = if opts.django_style {
        make_django_style_table_name(&opts.excel, &table.name)
    } else {
        table.name.clone()
    };

    check_table_exists(&table_name, pool.as_ref())?;

    if opts.clear {
        pool.prep_exec(format!("DELETE FROM {}", table_name), ())?;
    }

    let mut count = 0;
    for row in table.iter_rows(opts.skip)? {
        if let Ok(r) = row {
            count += 1;
            let sql = make_insert_sql(&table_name, &table.fields, &r);
            pool.prep_exec(sql, ())?;
        } else {
            warn!("Invalid row of {}: {:?}", table_name, row);
        }
    }

    Ok((count, table_name))
}

// insert into table_name (`c1`, `c2`, `c3`, `c4`) values (`:1`, `:2`, `:3`, `:4`);
pub fn make_insert_sql(table_name: &str, fields: &Vec<String>, row: &Vec<DataType>) -> String {
    let mut result: String = format!("INSERT INTO `{}` (", table_name);

    let fields_len = fields.len();
    for i in 0..fields.len() {
        result.push_str("`");
        result.push_str(&fields[i]);
        result.push_str("`");

        if i != fields_len - 1 {
            result.push_str(", ");
        }
    }

    result.push_str(") VALUES (");

    let row_len = row.len();
    for i in 0..row_len {
        match &row[i] {
            DataType::String(v) => result.push_str(format!("\"{}\"", v).as_str()),
            DataType::Bool(v) => result.push_str(format!("{}", *v as i32).as_str()),
            DataType::Int(v) => result.push_str(format!("{}", v).as_str()),
            DataType::Float(v) => result.push_str(format!("{}", v).as_str()),
            _ => result.push_str("null"),
        }

        if i != row_len - 1 {
            result.push_str(", ");
        }
    }

    result.push_str(");");

    result
}

pub fn check_table_exists(table_name: &str, pool: &Pool) -> Result<(), MySQLError> {
    let result = pool.first_exec(format!("SHOW TABLES LIKE \"{}\"", table_name), ())?;

    if result.is_none() {
        Err(MySQLError::from(mysql::MySqlError {
            state: String::from("-1"),
            message: format!("Table '{}' doesn't exist", table_name),
            code: 99,
        }))
    } else {
        Ok(())
    }
}

pub fn make_django_style_table_name(filepath: &str, table_name: &str) -> String {
    let file_stem = Path::new(filepath)
        .file_stem()
        .or(Some(OsStr::new("")))
        .and_then(|os_str| os_str.to_str())
        .unwrap_or("");

    let mut result = String::new();
    result.push_str(file_stem);
    result.push_str("_");
    result.push_str(table_name.to_lowercase().as_str());
    result
}

pub fn parse() {
    let opts = Arc::new(Opts::from_args());

    let mut builder = mysql::OptsBuilder::new();
    builder
        .db_name(Some(&opts.database))
        .ip_or_hostname(Some(&opts.host))
        .user(Some(&opts.user))
        .tcp_port(opts.port)
        .pass(Some(&opts.password));

    let pool = Arc::new(Pool::new(mysql::Opts::from(builder)).unwrap());
    let parse_result = parse_excel(&opts.excel);

    if let Ok(result) = parse_result {
        for mut table in result.into_iter() {
            if opts.django_style {
                table.to_django_style_fields();
            }

            let cloned_opts = opts.clone();
            let cloned_pool = pool.clone();

            let th = thread::spawn(move || {
                let result = import_table_to_database(cloned_opts, cloned_pool, table);
                match result {
                    Ok(r) => println!("Import {} rows for {}", r.0, r.1),
                    Err(e) => println!("ERROR:> {}", e),
                }
            });

            th.join().unwrap();
        }
    } else {
        println!("ERROR: {}", parse_result.unwrap_err());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_excel() {
        let result = parse_excel("manifest/main.xlsx");
        if let Ok(r) = result {
            assert_eq!(r.len(), 8);

            if let Ok(_row) = &r[1].iter_rows(1) {
            } else {
                panic!("Invalid first table")
            }
            println!("{:?}", &r[1].fields)
        } else {
            panic!("{}", result.unwrap_err())
        }
    }

    #[test]
    fn test_make_django_style_table_name_v1() {
        let filepath = "/root/developenv/rustlang/excel2sql/manifest/main.xlsx";

        if let Ok(table) = Table::new("Video", Range::empty()) {
            let result = make_django_style_table_name(filepath, &table.name);
            assert_eq!(result, "main_video")
        } else {
            panic!("Invalid table")
        }
    }

    #[test]
    fn test_make_django_style_table_name_v2() {
        let filepath = "/root/developenv/rustlang/excel2sql/manifest/platform.xlsx";

        if let Ok(table) = Table::new("KeyValue", Range::empty()) {
            let result = make_django_style_table_name(filepath, &table.name);
            assert_eq!(result, "platform_keyvalue")
        } else {
            panic!("Invalid table")
        }
    }

    #[test]
    fn test_check_table_exists_v1() {
        let mut builder = mysql::OptsBuilder::new();
        builder
            .db_name(Some("UBOX_english_hn_lt"))
            .ip_or_hostname(Some("localhost"))
            .user(Some("root"))
            .pass(Some("123456"));

        let pool = Pool::new(mysql::Opts::from(builder)).unwrap();

        assert!(check_table_exists("main_video", &pool).is_ok());
        assert!(check_table_exists("haha", &pool).is_err());
    }

    #[test]
    fn test_make_insert_sql_v1() {
        let sql = make_insert_sql(
            "UBOX_english_hn_lt",
            &vec![
                String::from("id"),
                String::from("name"),
                String::from("age"),
            ],
            &vec![
                DataType::Int(1),
                DataType::String(String::from("Tom")),
                DataType::Int(12),
            ],
        );
        assert_eq!(
            sql,
            "INSERT INTO `UBOX_english_hn_lt` (`id`, `name`, `age`) VALUES (1, \"Tom\", 12);"
        )
    }
}
