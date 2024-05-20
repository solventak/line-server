// Think about the text file like a database
// Keep an index of the file in memory and consider the file to be on disk storage
//   - the index will be a hashmap of line number to byte offset
//   - there are no writes because the data is immutable
//   - the index will be built on startup (persisted for later)
//   - the index will be built by reading the file line by line and storing the byte offset of the start of the line
//   - the index will be used to seek to the correct byte offset in the file to read the line

// Frame:
// | Command | Command Args | Checksum |
// 0x0 is GET
// 0x1 is QUIT
// 0x2 is SHUTDOWN
// only command that has args is GET which is a u32.  if it is none then we will just send 0 because the first line in the file is 1 indexed

// because the file is immutable we're not going to have to write to the index
// after the first time that we read in the file and built it.

// example GET
// 0x00 | 0x00 0x00 0x00 0x01 | 0x00 | 0x0A
mod db;
mod frame;

use anyhow::Result;
use db::Session;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

use frame::{Command, Frame, FrameError};
use std::collections::HashMap;

use fern;
use log::{self, error, warn};
use log::info;
use tokio::sync::{broadcast, mpsc};

static SERIALIZE_INDEX: bool = true;
static PORT: u16 = 10497;

fn setup_logger() -> Result<(), fern::InitError> {
    let log_file = "output.log";

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} [{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(std::io::stdout())
        .chain(fern::log_file(log_file)?)
        .apply()?;
    Ok(())
}

async fn shutdown_thread(mut cmd_rx: mpsc::Receiver<()>, shutdown_tx: broadcast::Sender<()>) {
    cmd_rx.recv().await;
    match shutdown_tx.send(()) {
        Ok(_) => info!("Shutdown signal sent."),
        Err(_) => {
            error!("Failed to send shutdown signal. Forcing shutdown.");
            std::process::exit(1);
        }
    }
}

struct Server {
    db: db::Database,
    active_connections: HashMap<String, tokio::task::JoinHandle<()>>,
}

impl Server {
    pub async fn new(db_fn: &str) -> Result<Server> {
        let db =
            db::Database::new(db_fn, format!("{db_fn}.index").as_str(), SERIALIZE_INDEX).await?;
        Ok(Server {
            db,
            active_connections: HashMap::new(),
        })
    }

    fn reap_finished_connections(&mut self) {
        let mut finished_connections = Vec::new();
        let active_connection_iter = self.active_connections.iter();
        // for each value in active connections try to join it
        for (conn_id, handle) in active_connection_iter {
            if handle.is_finished() {
                finished_connections.push(conn_id.clone());
            }
        }
        // remove the finished connections
        for conn_id in finished_connections {
            self.active_connections.remove(&conn_id);
        }
    }

    async fn finish_active_connections(&mut self) {
        let active_conn_ids = self
            .active_connections
            .keys()
            .cloned()
            .collect::<Vec<String>>();
        for conn_id in active_conn_ids {
            if let Some(handle) = self.active_connections.remove(&conn_id) {
                if let Err(_) = handle.await {
                    warn!("Tried to shut down connection {} but its thread was either cancelled or panicked.", conn_id);
                }
            }
        }
    }

    pub async fn run(&mut self) {
        // init the TCP listener
        let listener = TcpListener::bind(format!("0.0.0.0:{PORT}").as_str())
            .await
            .expect(format!("Could not bind to port {PORT}").as_str());
        // init channels
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let (cmd_tx, cmd_rx) = mpsc::channel::<()>(1);
        // start the shutdown thread
        tokio::spawn(shutdown_thread(cmd_rx, shutdown_tx.clone()));
        let mut master_shutdown_subscriber = shutdown_tx.subscribe();
        loop {
            match tokio::time::timeout(tokio::time::Duration::from_millis(100), listener.accept())
                .await
            {
                Err(_) => {
                    self.reap_finished_connections();
                    if let Ok(()) = master_shutdown_subscriber.try_recv() {
                        self.finish_active_connections().await;
                        info!("Server shutting down.  Goodbye!");
                        return;
                    }
                }
                Ok(listen_result) => match listen_result {
                    Ok((tcpstream, _addr)) => {
                        let mut connection = Connection::new(
                            tcpstream,
                            &self.db,
                            shutdown_tx.clone(),
                            cmd_tx.clone(),
                        )
                        .await;
                        self.active_connections.insert(
                            connection.conn_id.clone(),
                            tokio::spawn(async move {
                                if let Err(e) = connection.run().await {
                                    warn!(
                                        "Error running connection {}: {:?}",
                                        connection.conn_id, e
                                    );
                                }
                            }),
                        );
                    }
                    Err(e) => {
                        warn!("Error accepting connection: {:?}", e);
                    }
                },
            }
        }
    }
}

enum FrameAction {
    Continue,
    EndConnection,
}

struct Connection {
    conn_id: String,
    shutdown_rx: broadcast::Receiver<()>,
    cmd_tx: mpsc::Sender<()>,
    reader: BufReader<TcpStream>,
    session: Session,
}

impl Connection {
    pub async fn new(
        stream: TcpStream,
        db: &db::Database,
        shutdown_tx: broadcast::Sender<()>,
        cmd_tx: mpsc::Sender<()>,
    ) -> Connection {
        Connection {
            conn_id: uuid::Uuid::new_v4().to_string(),
            shutdown_rx: shutdown_tx.subscribe(),
            cmd_tx,
            reader: BufReader::new(stream),
            session: db.get_session().await.expect(
                "Could not get a session from the database. Database file missing or corrupted.",
            ),
        }
    }

    async fn handle_frame(&mut self, buf: Vec<u8>) -> Result<FrameAction> {
        // handle frame
        let frame = match Frame::try_from(&buf[..]) {
            Ok(frame) => frame,
            Err(FrameError::ClientDisconnected) => {
                warn!("Lost connection from {} unexpectedly.", self.conn_id);
                return Ok(FrameAction::EndConnection);
            }
            Err(_e) => {
                if let Err(e) = self.reader.get_mut().write_all(b"ERR\r\n").await {
                    warn!("Error writing to client: {:?}", e);
                    return Ok(FrameAction::EndConnection);
                }
                return Ok(FrameAction::Continue);
            }
        };

        match frame.cmd {
            Command::Get(line_number) => {
                info!("{} - GET {}", self.conn_id, line_number);
                match self.session.get(line_number as u64).await {
                    Ok(line) => {
                        self.reader.get_mut().write_all(b"OK\r\n").await?;
                        self.reader.get_mut().write_all(line.as_bytes()).await?;
                    }
                    Err(_) => {
                        self.reader.get_mut().write_all(b"ERR\r\n").await?;
                    }
                }
                Ok(FrameAction::Continue)
            }
            Command::Quit => {
                info!("{} - QUIT", self.conn_id);
                let _ = self.reader.get_mut().shutdown().await;
                Ok(FrameAction::EndConnection)
            }
            Command::Shutdown => {
                info!("{} - SHUTDOWN", self.conn_id);
                if let Err(_) = self.cmd_tx.send(()).await {
                    error!("Failed to send shutdown signal to server. Forcing shutdown.");
                    std::process::exit(1);
                }
                Ok(FrameAction::EndConnection)
            }
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            // get next message on stream
            let mut buf = Vec::new();
            self.reader.read_until(0xA, &mut buf).await?;

            // if we received a shutdown signal, then shutdown the client and break the loop, which shuts down the connection
            if self.shutdown_rx.try_recv().is_ok() {
                self.reader.get_mut().write_all(b"SHUTDOWN\r\n").await?;
                break;
            }

            match self.handle_frame(buf).await {
                Ok(FrameAction::EndConnection) => break,
                Ok(FrameAction::Continue) => continue,
                Err(_) => {
                    println!("got an error with a connection frame");
                    break;
                }
            }
        }
        info!("Server disconnects from {}", self.conn_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // get the db filename from the command line arg
    let args: Vec<String> = std::env::args().collect();
    let db_fn = &args[1];
    setup_logger().expect("could not set up logger");
    let mut server = Server::new(db_fn)
        .await
        .expect("Error creating server... exiting.");
    server.run().await;
}
