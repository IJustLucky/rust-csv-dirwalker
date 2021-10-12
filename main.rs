use walkdir::WalkDir;
use std::ffi::OsStr;
use rusqlite::{params, Connection, Result, NO_PARAMS};
use std::error::Error;
use std::io;
use std::process;
use std::path::Path;
use serde::{Serialize, Deserialize};
use csv;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    id: usize,
    price: f64,
    quantity: f64,
    quoted_quantity: f64,
    time: i64,
    is_buyer_maker: bool,
    is_best_match: bool,
}

fn process_csv(file: &Path, db: &Connection) -> Result<(), Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path(file)?;
    for result in rdr.deserialize::<Trade>() {
        match result {
            Ok(s) => {
                db.execute("INSERT INTO trades (id, price, quantity, quoted_quantity, time, is_buyer_maker, is_best_match) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                           params![s.id, s.price, s.quantity, s.quoted_quantity, s.time, s.is_buyer_maker, s.is_best_match])?;

            },
            Err(e) => println!("Failed to deserialize: {}", e),
        }
    }
    Ok(())
}

fn main () -> Result<(), Box<dyn Error>> {
    let conn = Connection::open("bot.db")?;
    conn.execute(
        "CREATE TABLE trades (
                  id              INTEGER PRIMARY KEY,
                  price           REAL NOT NULL,
                  quantity         REAL NOT NULL,
                  quoted_quantity  REAL NOT NULL,
                  time            INTEGER NOT NULL,
                  is_buyer_maker  INTEGER NOT NULL,
                  is_best_match   INTEGER NOT NULL,
                  )"
        [],
    )?;
        println!("Created table");
    for entry in WalkDir::new("E:/binance-public-data/python/data/spot/monthly/trades/").into_iter().filter_map(|e| e.ok()) {
        println!("Processing: {}", entry.path().display());
        if let Err(e) = process_csv(entry.path(), &conn) {
            println!("Processing failed: {}", e);
        }
    }
    Ok(())
}
