//! `ruvio-ctl import`: copies keys from a running Redis (or Ruvio) into Ruvio with ordinary
//! commands. It scans the source, reads each key by type, writes it to the target and
//! keeps the TTL. It never touches RDB files and works with any source version.

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    time::Duration,
};

const PAGE: usize = 1_000;

#[derive(Debug, Clone, Default)]
pub struct ImportOptions {
    pub from: String,
    pub to: String,
    pub pattern: Option<String>,
    pub batch: usize,
    pub replace: bool,
    pub dry_run: bool,
    pub verify: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ImportReport {
    pub scanned: u64,
    pub copied: u64,
    pub skipped_existing: u64,
    pub vanished: u64,
    pub unsupported: u64,
    pub verified: u64,
    pub mismatched: u64,
    pub by_type: [(&'static str, u64); 6],
}

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

impl Resp {
    fn bytes(&self) -> Result<&[u8], String> {
        match self {
            Resp::Bulk(Some(bytes)) | Resp::Simple(bytes) => Ok(bytes),
            other => Err(format!("expected a string reply, got {other:?}")),
        }
    }

    fn int(&self) -> Result<i64, String> {
        match self {
            Resp::Int(value) => Ok(*value),
            other => Err(format!("expected an integer reply, got {other:?}")),
        }
    }

    fn items(&self) -> Result<&[Resp], String> {
        match self {
            Resp::Array(Some(items)) => Ok(items),
            Resp::Array(None) => Ok(&[]),
            other => Err(format!("expected an array reply, got {other:?}")),
        }
    }
}

struct Connection {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

/// `redis://[user:password@]host:port[/db]`; `ruvio://` and a bare `host:port` also work.
pub struct Endpoint {
    pub address: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: u32,
}

pub fn parse_endpoint(url: &str) -> Result<Endpoint, String> {
    let rest = url
        .strip_prefix("redis://")
        .or_else(|| url.strip_prefix("ruvio://"))
        .unwrap_or(url);
    let (credentials, rest) = match rest.rsplit_once('@') {
        Some((credentials, rest)) => (Some(credentials), rest),
        None => (None, rest),
    };
    let (address, database) = match rest.split_once('/') {
        Some((address, "")) => (address, 0),
        Some((address, db)) => (
            address,
            db.parse().map_err(|_| format!("bad database in {url}"))?,
        ),
        None => (rest, 0),
    };

    if address.is_empty() {
        return Err(format!("no address in {url}"));
    }

    let (user, password) = match credentials {
        None => (None, None),
        Some(credentials) => match credentials.split_once(':') {
            Some((user, password)) => (
                Some(user.to_owned()).filter(|user| !user.is_empty()),
                Some(password.to_owned()),
            ),
            None => (None, Some(credentials.to_owned())),
        },
    };

    Ok(Endpoint {
        address: if address.contains(':') {
            address.to_owned()
        } else {
            format!("{address}:6379")
        },
        user,
        password,
        database,
    })
}

impl Connection {
    fn open(endpoint: &Endpoint) -> Result<Self, String> {
        let stream = TcpStream::connect(&endpoint.address)
            .map_err(|error| format!("connect {}: {error}", endpoint.address))?;

        stream.set_nodelay(true).ok();
        stream.set_read_timeout(Some(Duration::from_secs(60))).ok();

        let mut connection = Self {
            reader: BufReader::new(stream.try_clone().map_err(|e| e.to_string())?),
            writer: stream,
        };

        if let Some(password) = &endpoint.password {
            let reply = match &endpoint.user {
                Some(user) => connection.call(&[b"AUTH", user.as_bytes(), password.as_bytes()])?,
                None => connection.call(&[b"AUTH", password.as_bytes()])?,
            };

            expect_ok(&reply, "AUTH")?;
        }

        if endpoint.database != 0 {
            let db = endpoint.database.to_string();

            expect_ok(&connection.call(&[b"SELECT", db.as_bytes()])?, "SELECT")?;
        }

        Ok(connection)
    }

    fn call(&mut self, args: &[&[u8]]) -> Result<Resp, String> {
        let mut replies = self.pipeline(&[args.iter().map(|arg| arg.to_vec()).collect()])?;

        match replies.pop() {
            Some(Resp::Error(message)) => Err(String::from_utf8_lossy(&message).into_owned()),
            Some(reply) => Ok(reply),
            None => Err("no reply".into()),
        }
    }

    /// Sends every command, then reads one reply for each. Error replies stay in the list.
    fn pipeline(&mut self, commands: &[Vec<Vec<u8>>]) -> Result<Vec<Resp>, String> {
        let mut wire = Vec::new();

        for command in commands {
            wire.extend_from_slice(format!("*{}\r\n", command.len()).as_bytes());

            for arg in command {
                wire.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
                wire.extend_from_slice(arg);
                wire.extend_from_slice(b"\r\n");
            }
        }

        self.writer
            .write_all(&wire)
            .map_err(|error| format!("write: {error}"))?;

        (0..commands.len()).map(|_| self.read_reply()).collect()
    }

    fn read_line(&mut self) -> Result<Vec<u8>, String> {
        let mut line = Vec::new();

        self.reader
            .read_until(b'\n', &mut line)
            .map_err(|error| format!("read: {error}"))?;

        if !line.ends_with(b"\r\n") {
            return Err("connection closed".into());
        }

        line.truncate(line.len() - 2);
        Ok(line)
    }

    fn read_reply(&mut self) -> Result<Resp, String> {
        let line = self.read_line()?;
        let (kind, rest) = line.split_first().ok_or("empty reply")?;
        let number = || -> Result<i64, String> {
            std::str::from_utf8(rest)
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or_else(|| "bad length in reply".to_owned())
        };

        Ok(match kind {
            b'+' => Resp::Simple(rest.to_vec()),
            b'-' => Resp::Error(rest.to_vec()),
            b':' => Resp::Int(number()?),
            b'$' => match number()? {
                length if length < 0 => Resp::Bulk(None),
                length => {
                    let mut body = vec![0; length as usize + 2];

                    self.reader
                        .read_exact(&mut body)
                        .map_err(|error| format!("read: {error}"))?;
                    body.truncate(length as usize);
                    Resp::Bulk(Some(body))
                }
            },
            b'*' => match number()? {
                length if length < 0 => Resp::Array(None),
                length => Resp::Array(Some(
                    (0..length)
                        .map(|_| self.read_reply())
                        .collect::<Result<_, _>>()?,
                )),
            },
            other => return Err(format!("unsupported reply type {:?}", *other as char)),
        })
    }
}

fn expect_ok(reply: &Resp, what: &str) -> Result<(), String> {
    match reply {
        Resp::Simple(text) if text == b"OK" => Ok(()),
        other => Err(format!("{what} failed: {other:?}")),
    }
}

fn command(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|part| part.to_vec()).collect()
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::type_complexity)]
enum Value {
    Text(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    Sorted(Vec<(Vec<u8>, f64)>),
    Stream(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>),
}

impl Value {
    /// Order-insensitive types sorted so two reads compare equal.
    fn normalized(mut self) -> Self {
        match &mut self {
            Value::Set(members) => members.sort(),
            Value::Hash(pairs) => pairs.sort(),
            Value::Sorted(entries) => entries.sort_by(|a, b| a.0.cmp(&b.0)),
            _ => {}
        }

        self
    }
}

const TYPES: [&str; 6] = ["string", "list", "set", "hash", "zset", "stream"];

fn read_value(source: &mut Connection, key: &[u8], kind: &str) -> Result<Value, String> {
    Ok(match kind {
        "string" => match source.call(&[b"GET", key])? {
            Resp::Bulk(Some(value)) => Value::Text(value),
            _ => return Err("string vanished".into()),
        },
        "list" => {
            let mut items = Vec::new();

            loop {
                let (from, to) = (
                    items.len().to_string(),
                    (items.len() + PAGE - 1).to_string(),
                );
                let page = source.call(&[b"LRANGE", key, from.as_bytes(), to.as_bytes()])?;
                let page = page.items()?;

                items.extend(
                    page.iter()
                        .map(|item| item.bytes().map(<[u8]>::to_vec))
                        .collect::<Result<Vec<_>, _>>()?,
                );

                if page.len() < PAGE {
                    break Value::List(items);
                }
            }
        }
        "set" | "hash" | "zset" => {
            let scan: &[u8] = match kind {
                "set" => b"SSCAN",
                "hash" => b"HSCAN",
                _ => b"ZSCAN",
            };
            let mut cursor = b"0".to_vec();
            let mut flat: Vec<Vec<u8>> = Vec::new();

            loop {
                let reply = source.call(&[scan, key, &cursor, b"COUNT", b"1000"])?;
                let parts = reply.items()?;

                cursor = parts.first().ok_or("bad SCAN reply")?.bytes()?.to_vec();
                for item in parts.get(1).ok_or("bad SCAN reply")?.items()? {
                    flat.push(item.bytes()?.to_vec());
                }

                if cursor == b"0" {
                    break;
                }
            }

            match kind {
                "set" => {
                    flat.sort();
                    flat.dedup();
                    Value::Set(flat)
                }
                "hash" => {
                    let mut pairs: Vec<_> = flat
                        .chunks(2)
                        .filter(|p| p.len() == 2)
                        .map(|p| (p[0].clone(), p[1].clone()))
                        .collect();

                    pairs.sort();
                    pairs.dedup_by(|a, b| a.0 == b.0);
                    Value::Hash(pairs)
                }
                _ => {
                    let mut entries = Vec::new();

                    for pair in flat.chunks(2).filter(|p| p.len() == 2) {
                        let score = std::str::from_utf8(&pair[1])
                            .ok()
                            .and_then(|text| text.parse::<f64>().ok())
                            .ok_or("bad zset score")?;

                        entries.push((pair[0].clone(), score));
                    }

                    entries.sort_by(|a, b| a.0.cmp(&b.0));
                    entries.dedup_by(|a, b| a.0 == b.0);
                    Value::Sorted(entries)
                }
            }
        }
        "stream" => {
            let mut entries = Vec::new();
            let mut start = b"-".to_vec();

            loop {
                let reply = source.call(&[b"XRANGE", key, &start, b"+", b"COUNT", b"1000"])?;
                let page = reply.items()?;

                for entry in page {
                    let parts = entry.items()?;
                    let id = parts.first().ok_or("bad XRANGE reply")?.bytes()?.to_vec();
                    let fields: Vec<Vec<u8>> = parts
                        .get(1)
                        .ok_or("bad XRANGE reply")?
                        .items()?
                        .iter()
                        .map(|field| field.bytes().map(<[u8]>::to_vec))
                        .collect::<Result<_, _>>()?;

                    entries.push((
                        id,
                        fields
                            .chunks(2)
                            .filter(|p| p.len() == 2)
                            .map(|p| (p[0].clone(), p[1].clone()))
                            .collect(),
                    ));
                }

                if page.len() < PAGE {
                    break;
                }

                start = next_stream_id(&entries.last().ok_or("empty page")?.0)?;
            }

            Value::Stream(entries)
        }
        other => return Err(format!("unsupported type {other}")),
    })
}

/// The id right after `id` (`ms-seq`). Used instead of the `(` exclusive bound, which Redis before 6.2
/// and Ruvio's XRANGE do not accept, so the importer works with any source.
fn next_stream_id(id: &[u8]) -> Result<Vec<u8>, String> {
    let text = std::str::from_utf8(id).map_err(|_| "bad stream id")?;
    let (millis, sequence) = text.split_once('-').unwrap_or((text, "0"));
    let millis: u64 = millis.parse().map_err(|_| "bad stream id")?;
    let sequence: u64 = sequence.parse().map_err(|_| "bad stream id")?;

    Ok(match sequence.checked_add(1) {
        Some(next) => format!("{millis}-{next}"),
        None => format!("{}-0", millis.checked_add(1).ok_or("stream id overflow")?),
    }
    .into_bytes())
}

fn write_value(target: &mut Connection, key: &[u8], value: &Value) -> Result<(), String> {
    let mut commands: Vec<Vec<Vec<u8>>> = Vec::new();

    match value {
        Value::Text(text) => commands.push(command(&[b"SET", key, text])),
        Value::List(items) => {
            for chunk in items.chunks(PAGE) {
                let mut cmd = command(&[b"RPUSH", key]);

                cmd.extend(chunk.iter().cloned());
                commands.push(cmd);
            }
        }
        Value::Set(members) => {
            for chunk in members.chunks(PAGE) {
                let mut cmd = command(&[b"SADD", key]);

                cmd.extend(chunk.iter().cloned());
                commands.push(cmd);
            }
        }
        Value::Hash(pairs) => {
            for chunk in pairs.chunks(PAGE) {
                let mut cmd = command(&[b"HSET", key]);

                for (field, value) in chunk {
                    cmd.push(field.clone());
                    cmd.push(value.clone());
                }

                commands.push(cmd);
            }
        }
        Value::Sorted(entries) => {
            for chunk in entries.chunks(PAGE) {
                let mut cmd = command(&[b"ZADD", key]);

                for (member, score) in chunk {
                    cmd.push(format!("{score}").into_bytes());
                    cmd.push(member.clone());
                }

                commands.push(cmd);
            }
        }
        Value::Stream(entries) => {
            for (id, fields) in entries {
                let mut cmd = command(&[b"XADD", key, id]);

                for (field, value) in fields {
                    cmd.push(field.clone());
                    cmd.push(value.clone());
                }

                commands.push(cmd);
            }
        }
    }

    for reply in target.pipeline(&commands)? {
        if let Resp::Error(message) = reply {
            return Err(String::from_utf8_lossy(&message).into_owned());
        }
    }

    Ok(())
}

/// Runs the import. `progress` is called after every scanned batch.
pub fn run(
    options: &ImportOptions,
    progress: &mut dyn FnMut(&ImportReport),
) -> Result<ImportReport, String> {
    let mut source = Connection::open(&parse_endpoint(&options.from)?)?;
    let mut target = Connection::open(&parse_endpoint(&options.to)?)?;
    let batch = options.batch.max(1).to_string();
    let mut report = ImportReport {
        by_type: TYPES.map(|name| (name, 0)),
        ..ImportReport::default()
    };
    let mut cursor = b"0".to_vec();

    loop {
        let mut scan = command(&[b"SCAN", &cursor]);

        if let Some(pattern) = &options.pattern {
            scan.extend([b"MATCH".to_vec(), pattern.clone().into_bytes()]);
        }

        scan.extend([b"COUNT".to_vec(), batch.clone().into_bytes()]);

        let reply = source.pipeline(&[scan])?.remove(0);
        let parts = reply.items()?;

        cursor = parts.first().ok_or("bad SCAN reply")?.bytes()?.to_vec();

        let mut keys: Vec<Vec<u8>> = Vec::new();

        for key in parts.get(1).ok_or("bad SCAN reply")?.items()? {
            keys.push(key.bytes()?.to_vec());
        }

        keys.sort();
        keys.dedup();
        report.scanned += keys.len() as u64;

        let probes: Vec<_> = keys
            .iter()
            .flat_map(|key| [command(&[b"TYPE", key]), command(&[b"PTTL", key])])
            .collect();
        let probed = source.pipeline(&probes)?;
        let existing = if options.replace || options.dry_run {
            vec![Resp::Int(0); keys.len()]
        } else {
            target.pipeline(
                &keys
                    .iter()
                    .map(|key| command(&[b"EXISTS", key]))
                    .collect::<Vec<_>>(),
            )?
        };

        for (index, key) in keys.iter().enumerate() {
            let kind = String::from_utf8_lossy(probed[index * 2].bytes()?).into_owned();
            let ttl = probed[index * 2 + 1].int()?;

            if kind == "none" || ttl == -2 {
                report.vanished += 1;
                continue;
            }

            let Some(slot) = TYPES.iter().position(|name| *name == kind) else {
                report.unsupported += 1;
                continue;
            };

            if existing[index].int().unwrap_or(0) != 0 {
                report.skipped_existing += 1;
                continue;
            }

            if !options.dry_run {
                let value = read_value(&mut source, key, &kind)?.normalized();

                if options.replace {
                    target.call(&[b"DEL", key])?;
                }

                write_value(&mut target, key, &value)?;

                if ttl > 0 {
                    target.call(&[b"PEXPIRE", key, ttl.to_string().as_bytes()])?;
                }

                if options.verify {
                    report.verified += 1;

                    if read_value(&mut target, key, &kind)?.normalized() != value {
                        report.mismatched += 1;
                    }
                }
            }

            report.copied += 1;
            report.by_type[slot].1 += 1;
        }

        progress(&report);

        if cursor == b"0" {
            return Ok(report);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_endpoints() {
        let plain = parse_endpoint("redis://cache.local:6380/2").unwrap();

        assert_eq!(
            (plain.address.as_str(), plain.database),
            ("cache.local:6380", 2)
        );
        assert!(plain.user.is_none() && plain.password.is_none());

        let secured = parse_endpoint("ruvio://alice:s3cret@10.0.0.5").unwrap();

        assert_eq!(secured.address, "10.0.0.5:6379");
        assert_eq!(secured.user.as_deref(), Some("alice"));
        assert_eq!(secured.password.as_deref(), Some("s3cret"));

        let password_only = parse_endpoint("redis://:pw@host:1").unwrap();

        assert_eq!(
            (password_only.user, password_only.password.as_deref()),
            (None, Some("pw"))
        );
        assert!(parse_endpoint("redis://").is_err());
        assert!(parse_endpoint("host:1/x").is_err());
    }

    #[test]
    fn normalizing_makes_unordered_reads_equal() {
        let a = Value::Set(vec![b"b".to_vec(), b"a".to_vec()]).normalized();
        let b = Value::Set(vec![b"a".to_vec(), b"b".to_vec()]).normalized();

        assert_eq!(a, b);
    }

    #[test]
    fn next_stream_id_moves_past_the_given_id() {
        assert_eq!(next_stream_id(b"5-7").unwrap(), b"5-8");
        assert_eq!(next_stream_id(b"5").unwrap(), b"5-1");
        assert_eq!(
            next_stream_id(format!("5-{}", u64::MAX).as_bytes()).unwrap(),
            b"6-0"
        );
        assert!(next_stream_id(format!("{}-{}", u64::MAX, u64::MAX).as_bytes()).is_err());
        assert!(next_stream_id(b"x-1").is_err());
        assert!(next_stream_id(b"1-y").is_err());
        assert!(next_stream_id(&[0xff]).is_err());
    }
}
