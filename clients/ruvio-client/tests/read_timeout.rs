//! A read timeout in the middle of a reply must not leave the connection half-read.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

mod common;

use ruvio_client::{Client, ClientOptions, Error};

#[test]
fn a_timeout_inside_a_frame_marks_the_connection_broken_instead_of_desyncing() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut socket, _) = common::accept_within_deadline(&listener);
        let mut buffer = [0_u8; 256];

        // Discovery asks CLUSTER SLOTS; a standalone server answers with an error.
        let _ = socket.read(&mut buffer).unwrap();
        socket
            .write_all(b"-ERR cluster support disabled\r\n")
            .unwrap();
        socket.write_all(b"$5\r\nab").unwrap();
        socket.flush().unwrap();
        thread::sleep(Duration::from_millis(400));
        let _ = socket.write_all(b"cde\r\n");
        thread::sleep(Duration::from_millis(200));
    });

    let mut client = Client::connect_with(ClientOptions {
        host: "127.0.0.1".to_owned(),
        port,
        connect_timeout: Duration::from_secs(1),
        reconnect: false,
        ..ClientOptions::default()
    })
    .unwrap();

    client
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();

    assert!(
        matches!(client.read_message(), Err(Error::Io(_))),
        "the frame is incomplete"
    );
    thread::sleep(Duration::from_millis(500)); // the rest of the frame arrives now

    // The late bytes `cde\r\n` must not be parsed as a new reply.
    match client.read_message() {
        Err(Error::Io(_)) => {}
        other => panic!("expected the closed-connection error, got {other:?}"),
    }

    server.join().unwrap();
}
