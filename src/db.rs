use log::info;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek};
use std::sync::Arc;

use anyhow::Result;

pub struct Session {
    reader: BufReader<File>,
    index: Arc<HashMap<u64, u64>>,
}

impl Session {
    pub async fn new(reader: BufReader<File>, index: Arc<HashMap<u64, u64>>) -> Result<Session> {
        Ok(Session { reader, index })
    }

    pub async fn get(&mut self, line_number: u64) -> Result<String> {
        let byte_offset = self.index.get(&(line_number));
        match byte_offset {
            Some(offset) => {
                self.reader.seek(std::io::SeekFrom::Start(*offset))?;
                let mut line = String::new();
                self.reader.read_line(&mut line)?;
                Ok(line)
            }
            None => Err(anyhow::Error::msg("line number not found in index")),
        }
    }
}

pub struct Database {
    db_file: String,
    index: Arc<HashMap<u64, u64>>,
}

impl Database {
    async fn load_index(
        db_file: &str,
        index_filename: &str,
        serialize_index: bool,
    ) -> Result<HashMap<u64, u64>> {
        let serialized_index_file = index_filename;
        if serialize_index && std::path::Path::new(serialized_index_file).exists() {
            info!(
                "Loading the saved index from file: {}",
                serialized_index_file
            );
            // load the index from the file
            return Ok(rmp_serde::from_read(std::io::BufReader::new(
                std::fs::File::open(serialized_index_file)?,
            ))?);
        } else {
            // else create a new index
            Ok(Database::index(db_file, index_filename, serialize_index)?)
        }
    }

    fn index(db_file: &str, index_filename: &str, save: bool) -> Result<HashMap<u64, u64>> {
        info!("Creating a new index for the database file: {}", db_file);
        let mut file = File::open(db_file)?;
        let mut reader = BufReader::new(&mut file);
        let mut index = HashMap::<u64, u64>::new();

        // see dwith the first line and its offset
        index.insert(1, 0);
        // start at line 2 since we seeded with line 1
        let mut current_line = 2;

        let mut buf = Vec::new();
        while let Ok(num_bytes) = reader.read_until(0x0A, &mut buf) {
            if num_bytes == 0 {
                break;
            }
            index.insert(current_line, reader.stream_position()?); // TODO: handle the error here
            current_line += 1;
            buf = Vec::new();
        }

        if save {
            // save the index to a file
            info!("Saving the index to file: {}", index_filename);
            let mut file = std::fs::File::create(index_filename)?;
            // TODO: bufwriter? is that a thing here?
            rmp_serde::encode::write(&mut file, &index)?;
        }

        Ok(index)
    }

    pub async fn new(
        db_file: &str,
        index_filename: &str,
        serialize_index: bool,
    ) -> Result<Database> {
        // TODO: logging about the index whether it was loaded or created and saved etc.
        let index = Database::load_index(db_file, index_filename, serialize_index).await?;
        Ok(Database {
            db_file: String::from(db_file),
            index: Arc::new(index),
        })
    }

    pub async fn get_session(&self) -> Result<Session> {
        let file = File::open(&self.db_file)?;
        let reader = BufReader::new(file);
        Session::new(reader, self.index.clone()).await
    }
}
