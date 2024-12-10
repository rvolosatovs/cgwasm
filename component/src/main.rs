use std::io::Write as _;
use std::net::TcpListener;

use anyhow::Context as _;

fn main() -> anyhow::Result<()> {
    let sock = TcpListener::bind("0.0.0.0:8080").context("failed to bind")?;
    loop {
        match sock.accept() {
            Ok((mut conn, addr)) => {
                eprintln!("accepted conn from {addr}");
                if let Err(err) = conn.write_all(b"hello") {
                    eprintln!("failed to write `hello`: {err}");
                }
            }
            Err(err) => {
                eprintln!("failed to accept conn: {err}");
            }
        }
    }
}
