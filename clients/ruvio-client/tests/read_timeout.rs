//! A read timeout in the middle of a reply must not leave the connection half-read.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

mod common;

use ruvio_client::{Client, ClientOptions, Error};

/// How long either side waits for the other before the test fails.
const DEADLINE: Duration = Duration::from_secs(10);

#[test]
fn a_timeout_inside_a_frame_marks_the_connection_broken_instead_of_desyncing() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    // The test and the server take turns through these, so no sleep decides the order.
    let (timed_out, wait_timed_out) = mpsc::channel::<()>();
    let (sent_rest, wait_sent_rest) = mpsc::channel::<()>();
    let (done, wait_done) = mpsc::channel::<()>();
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
        // The rest of the frame goes out only after the client's read timed out.
        wait_timed_out.recv_timeout(DEADLINE).unwrap();
        let _ = socket.write_all(b"cde\r\n");
        let _ = socket.flush();
        sent_rest.send(()).unwrap();
        // Kept open until the client's last read, so that read cannot fail on a closed socket.
        let _ = wait_done.recv_timeout(DEADLINE);
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
    timed_out.send(()).unwrap();
    wait_sent_rest.recv_timeout(DEADLINE).unwrap();
    // Gives the late bytes, already written, time to reach the client's socket.
    thread::sleep(Duration::from_millis(50));

    // The late bytes `cde\r\n` must not be parsed as a new reply.
    match client.read_message() {
        Err(Error::Io(_)) => {}
        other => panic!("expected the closed-connection error, got {other:?}"),
    }

    done.send(()).unwrap();
    server.join().unwrap();
}
