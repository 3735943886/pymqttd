//! An MQTT 3.1.1 broker (rumqttd) configured from the command line, for an application that
//! starts it as a child process.
//!
//! Once it accepts connections it prints one line to stdout, `ready <host>:<port>`, so the parent
//! need not poll. With `--exit-on-stdin-close` it exits when its stdin reaches end of file: the
//! parent holds the pipe, and the broker goes when the parent does, however the parent ends.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use clap::Parser;
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on.
    #[arg(long, default_value_t = 1883)]
    port: u16,

    /// Require this username (with --password); anonymous clients are refused.
    #[arg(long, requires = "password")]
    username: Option<String>,

    /// The password for --username.
    #[arg(long, requires = "username")]
    password: Option<String>,

    /// Largest accepted PUBLISH payload, in bytes.
    #[arg(long, default_value_t = 1 << 20)]
    max_payload_size: usize,

    /// Most clients connected at once.
    #[arg(long, default_value_t = 1000)]
    max_connections: usize,

    /// Exit when stdin reaches end of file.
    #[arg(long)]
    exit_on_stdin_close: bool,

    /// rumqttd's log filter for stderr (`warn`, `rumqttd=debug`, ...). Off by default: rumqttd logs
    /// every client disconnect, clean ones too, as an error. pymqttd's own errors go to stderr
    /// regardless.
    #[arg(long, default_value = "off")]
    log: String,
}

fn config(args: &Args, listen: SocketAddr) -> Config {
    let auth = args
        .username
        .as_ref()
        .zip(args.password.as_ref())
        .map(|(user, pass)| HashMap::from([(user.clone(), pass.clone())]));
    let server = ServerSettings {
        name: "v4".into(),
        listen,
        tls: None,
        next_connection_delay_ms: 1,
        connections: ConnectionSettings {
            connection_timeout_ms: 60_000,
            max_payload_size: args.max_payload_size,
            max_inflight_count: 100,
            auth,
            external_auth: None,
            dynamic_filters: true,
        },
    };
    Config {
        id: 0,
        router: RouterConfig {
            max_connections: args.max_connections,
            max_outgoing_packet_count: 200,
            max_segment_size: 1 << 20,
            max_segment_count: 10,
            ..RouterConfig::default()
        },
        v4: Some(HashMap::from([("1".into(), server)])),
        ..Config::default()
    }
}

/// A clean MQTT 3.1.1 session against `addr`: CONNECT, CONNACK, DISCONNECT. Any CONNACK counts,
/// a refused one (wrong credentials) too: something is answering MQTT there.
fn mqtt_handshake(addr: SocketAddr, credentials: Option<(&str, &str)>) -> std::io::Result<()> {
    fn string(out: &mut Vec<u8>, s: &str) {
        out.extend_from_slice(&u16::try_from(s.len()).unwrap_or(u16::MAX).to_be_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    let mut body = Vec::new();
    string(&mut body, "MQTT");
    body.push(4); // protocol level 3.1.1
    body.push(0x02 | credentials.map_or(0, |_| 0xC0)); // clean session (+ username, password)
    body.extend_from_slice(&10u16.to_be_bytes()); // keep alive
    string(&mut body, &format!("pymqttd-ready-{}", std::process::id()));
    if let Some((user, pass)) = credentials {
        string(&mut body, user);
        string(&mut body, pass);
    }
    let len = u8::try_from(body.len()).map_err(|_| std::io::Error::other("credentials too long"))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(200))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(&[0x10, len])?;
    stream.write_all(&body)?;
    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack)?;
    if connack[0] != 0x20 {
        return Err(std::io::Error::other("not an MQTT broker"));
    }
    stream.write_all(&[0xE0, 0x00])
}

/// Prints `ready <addr>` once the broker answers on `probe`; exits the process if it never does.
fn announce_when_listening(listen: SocketAddr, probe: SocketAddr, credentials: Option<(String, String)>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let credentials = credentials.as_ref().map(|(u, p)| (u.as_str(), p.as_str()));
    loop {
        if mqtt_handshake(probe, credentials).is_ok() {
            let mut out = std::io::stdout().lock();
            let _ = writeln!(out, "ready {listen}");
            let _ = out.flush();
            return;
        }
        if Instant::now() >= deadline {
            eprintln!("pymqttd: not answering on {probe} after 10 s");
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn exit_on_stdin_close() {
    let mut buf = [0u8; 256];
    let mut stdin = std::io::stdin().lock();
    loop {
        match stdin.read(&mut buf) {
            Ok(0) | Err(_) => std::process::exit(0),
            Ok(_) => {}
        }
    }
}

fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::new(&args.log))
        .init();

    let listen: SocketAddr = match format!("{}:{}", args.host, args.port).parse() {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("pymqttd: bad listen address {}:{}: {err}", args.host, args.port);
            std::process::exit(2);
        }
    };
    // The ready probe cannot tell this broker from another one on the same port: fail here instead
    // when the port is taken. (Something could still take it before the broker binds; then `start`
    // returns and this exits non-zero.)
    if let Err(err) = std::net::TcpListener::bind(listen) {
        eprintln!("pymqttd: cannot listen on {listen}: {err}");
        std::process::exit(1);
    }
    if args.exit_on_stdin_close {
        std::thread::spawn(exit_on_stdin_close);
    }
    // A wildcard address is reached through loopback.
    let probe = match listen.ip().is_unspecified() {
        true if listen.is_ipv4() => SocketAddr::from(([127, 0, 0, 1], listen.port())),
        true => SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, listen.port())),
        false => listen,
    };
    let credentials = args.username.clone().zip(args.password.clone());
    std::thread::spawn(move || announce_when_listening(listen, probe, credentials));

    let mut broker = Broker::new(config(&args, listen));
    // `start` blocks while the servers run; it returns only when they have stopped (a listen
    // address already in use, say).
    match broker.start() {
        Ok(()) => eprintln!("pymqttd: the broker stopped"),
        Err(err) => eprintln!("pymqttd: {err}"),
    }
    std::process::exit(1);
}
