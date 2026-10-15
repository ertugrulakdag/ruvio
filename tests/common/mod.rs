//! Helpers shared by the live integration tests: a real `ruvio` process on a free port and a tiny RESP client.
#![allow(dead_code)]

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

pub struct Server {
    child: Option<Child>,
    pub address: String,
    pub data_dir: PathBuf,
    config: PathBuf,
    env: Vec<(String, String)>,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

impl Server {
    pub fn start() -> Self {
        Self::start_with(&[])
    }

    /// `env` entries are passed as `RUVIO_*` settings (for example `("RUVIO_DURABILITY", "always")`).
    pub fn start_with(env: &[(&str, &str)]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        // The template is copied: a server started with --address rewrites its config file.
        let config = dir.path().join("ruvio.toml");

        std::fs::copy(concat!(env!("CARGO_MANIFEST_DIR"), "/ruvio.example.toml"), &config).unwrap();

        let mut server = Self {
            child: None,
            address: format!("127.0.0.1:{}", free_port()),
            data_dir: dir.path().join("data"),
            config,
            env: env
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            _dir: dir,
        };

        server.spawn();
        server
    }

    fn spawn(&mut self) {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ruvio"));

        command
            .arg("--config")
            .arg(&self.config)
            .args(["--address", &self.address, "--data-dir"])
            .arg(&self.data_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        for (key, value) in &self.env {
            command.env(key, value);
        }

        self.child = Some(command.spawn().unwrap());

        let deadline = Instant::now() + Duration::from_secs(20);

        while TcpStream::connect(&self.address).is_err() {
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Kills the process without a clean shutdown (like a crash).
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Starts the same server again on the same address and data directory.
    pub fn restart(&mut self) {
        self.stop();
        self.spawn();
    }
}

pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    pub fn connect(address: &str) -> Self {
        let stream = TcpStream::connect(address).unwrap();

        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();

        Self {
            writer: stream.try_clone().unwrap(),
            reader: BufReader::new(stream),
        }
    }

    pub fn call(&mut self, args: &[&str]) -> String {
        self.send(args);
        self.read()
    }

    pub fn send(&mut self, args: &[&str]) {
        let mut frame = format!("*{}\r\n", args.len());

        for arg in args {
            frame.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }

        self.writer.write_all(frame.as_bytes()).unwrap();
    }

    /// Renders a reply as text: integers and simple strings as is, bulk as its payload, nil as
    /// `(nil)`, errors as `ERR:<message>`, arrays and pushes as `[a,b]`.
    pub fn read(&mut self) -> String {
        let mut line = String::new();

        self.reader.read_line(&mut line).unwrap();

        let line = line.trim_end().to_owned();
        let (kind, rest) = line.split_at(1);

        match kind {
            "+" | ":" => rest.to_owned(),
            "-" => format!("ERR:{rest}"),
            "_" => "(nil)".to_owned(),
            "$" => {
                let len: i64 = rest.parse().unwrap();

                if len < 0 {
                    return "(nil)".to_owned();
                }

                let mut payload = vec![0; len as usize + 2];

                self.reader.read_exact(&mut payload).unwrap();
                String::from_utf8_lossy(&payload[..len as usize]).into_owned()
            }
            "*" | ">" => {
                let count: i64 = rest.parse().unwrap();
                let items: Vec<String> = (0..count.max(0)).map(|_| self.read()).collect();

                format!("[{}]", items.join(","))
            }
            other => panic!("unexpected reply type {other}"),
        }
    }
}
