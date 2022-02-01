use std::fmt::Display;
use std::future::Future;
use std::net::IpAddr;
use bytes::{Bytes, BytesMut};
use rml_rtmp::chunk_io::Packet;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult};

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;

pub struct RtmpConnection {
    ip: IpAddr,
    session: Option<ServerSession>
}

impl RtmpConnection {
    pub async fn handshake(self, mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
        let mut handshake = Handshake::new(PeerType::Server);

        let server_p0_and_1 = handshake.generate_outbound_p0_and_p1()
            .map_err(|err| format!("Conn {}: Failed to generate RTMP p0p1 handshake: {:?}", self.ip, err))?;

        stream.write_all(&server_p0_and_1).await?;

        let mut buffer = [0; 4096];
        loop {
            let bytes_read = stream.read(&mut buffer).await?;

            if bytes_read == 0 {
                // We reached the end of the handshake
                return Ok(());
            }

            match handshake.process_bytes(&buffer[0..bytes_read])
                .map_err(|err| format!("Conn {}: Failed to parse handshake body: {:?}", self.ip, err))? {

                HandshakeProcessResult::InProgress { response_bytes } => {
                    stream.write_all(&response_bytes).await?;
                }

                HandshakeProcessResult::Completed { response_bytes, remaining_bytes} => {
                    stream.write_all(&response_bytes).await?;
                    spawn(self.start_connection_manager(stream, remaining_bytes));
                    return Ok(());
                }
            }
        }
    }

    async fn start_connection_manager(mut self, stream: TcpStream, received_bytes: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream_reader, stream_writer) = tokio::io::split(stream);

        let (read_bytes_sender, mut read_bytes_receiver) = mpsc::unbounded_channel();
        let (mut write_bytes_sender, write_bytes_receiver) = mpsc::unbounded_channel();

        spawn(connection_reader(stream_reader, read_bytes_sender, self.ip));
        spawn(connection_writer(stream_writer, write_bytes_receiver, self.ip));

        let config = ServerSessionConfig::new();
        let (session, mut results) = ServerSession::new(config)
            .map_err(|err| format!("Conn {}: Failed to create server session: {:?}", self.ip, err))?;

        self.session = Some(session);

        let remaining_bytes_results = self.session.as_mut().unwrap()
            .handle_input(&received_bytes)
            .map_err(|err| format!("Conn {}: Failed to handle input: {:?}", self.ip, err))?;

        results.extend(remaining_bytes_results);

        loop {
            if !self.handle_session_results(&mut results, &mut write_bytes_sender)? {
                break;
            }

            match read_bytes_receiver.recv().await {
                None => break,
                Some(bytes) => {
                    results = self.session.as_mut().unwrap()
                        .handle_input(&bytes)
                        .map_err(|err| format!("Conn {}: Error handling input: {:?}", self.ip, err))?;
                }
            }
        }

        println!("Conn {}: Client disconnected", self.ip);

        Ok(())
    }

    fn handle_session_results(&mut self, results: &mut Vec<ServerSessionResult>, byte_writer: &mut UnboundedSender<Packet>)
                              -> Result<bool, Box<dyn std::error::Error + Sync + Send>> {
        if results.len() == 0 {
            return Ok(true);
        }

        let mut new_results = Vec::new();
        for result in results.drain(..) {
            //println!("Handling result: {:?}", result);
            match result {
                ServerSessionResult::OutboundResponse(packet) => {
                    if !send(&byte_writer, packet) {
                        break;
                    }
                }
                ServerSessionResult::RaisedEvent(event) => {
                    if !self.handle_raised_event(event, &mut new_results)? {
                        return Ok(false)
                    }
                }
                ServerSessionResult::UnhandleableMessageReceived(payload) => {
                    println!("Conn {}: Unable to handle message: {:?}", self.ip, payload);
                }
            }
        }
        self.handle_session_results(&mut new_results, byte_writer)?;

        Ok(true)
    }

    fn handle_raised_event(&mut self, event: ServerSessionEvent, new_results: &mut Vec<ServerSessionResult>) -> Result<bool, Box<dyn std::error::Error + Sync + Send>> {
        match event {
            ServerSessionEvent::ConnectionRequested { request_id, app_name} => {
                println!("Conn {}: Client requested connection to app {:?}", self.ip, app_name);

                new_results.extend(self.session.as_mut().unwrap()
                    .accept_request(request_id)
                    .map_err(|err| format!("Conn {}: Error accepting request: {:?}", self.ip, err))?);
            }
            ServerSessionEvent::PublishStreamRequested { request_id, app_name, mode, stream_key } => {
                println!("Conn {}: Client requested publishing on {}/{} in mode {:?}", self.ip, app_name, stream_key, mode);

                new_results.extend(self.session.as_mut().unwrap()
                    .accept_request(request_id)
                    .map_err(|err| format!("Conn {}: Error accepting request: {:?}", self.ip, err))?);
            }
            ServerSessionEvent::PlayStreamRequested { request_id: _, app_name, stream_key, stream_id: _ , ..} => {
                println!("Conn {}: Client requesting playback on {}/{}", self.ip, app_name, stream_key);
                eprintln!("Conn {}: Playback not supported.", self.ip);
                return Ok(false);
            }
            ServerSessionEvent::StreamMetadataChanged { stream_key, app_name, metadata } => {
                println!("Conn {}: New metadata published on {}/{}: {:?}", self.ip, app_name, stream_key, metadata);
            }
            ServerSessionEvent::VideoDataReceived { app_name, stream_key, timestamp, data } => {
                println!("Conn {}: Received video data on {}/{}: timestamp={:?},len={}", self.ip, app_name, stream_key, timestamp, data.len());
            }
            ServerSessionEvent::AudioDataReceived { app_name, stream_key, timestamp, data} => {
                println!("Conn {}: Received audio data on {}/{}: timestamp={:?},len={}", self.ip, app_name, stream_key, timestamp, data.len());
            }
            ServerSessionEvent::PlayStreamFinished { .. } => {
                println!("Conn {}: Playback finished", self.ip);
                eprintln!("Conn {}: Playback not supported.", self.ip);
                return Ok(false);
            }
            ServerSessionEvent::PublishStreamFinished { .. } => {
                println!("Conn {}: Publish finished", self.ip);
            }
            e => println!("Conn {}: Unknown event raised: {:?}", self.ip, e)
        };

        Ok(true)
    }
}

async fn connection_reader(mut stream: ReadHalf<TcpStream>, manager: mpsc::UnboundedSender<Bytes>, ip: IpAddr)
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        let bytes_read = stream.read_buf(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }

        let bytes = buffer.split_off(bytes_read);
        if !send(&manager, buffer.freeze()) {
            break;
        }

        buffer = bytes;
    }

    println!("Conn {}: Reader disconnected", ip);
    Ok(())
}

async fn connection_writer(mut stream: WriteHalf<TcpStream>, mut packets_to_send: mpsc::UnboundedReceiver<Packet>, ip: IpAddr)
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    loop {
        let packet = packets_to_send.recv().await;
        if packet.is_none() {
            break; // connection closed
        }
        // We don't need to do some of the complex logic necessary to handle floods of packets b/c
        // we never send video/audio information back to clients, so only handle small-volume writes
        stream.write_all(packet.unwrap().bytes.as_ref()).await?;
    }

    println!("Conn {}: Writer disconnected", ip);
    Ok(())
}

pub(crate) async fn run_rtmp_server(listen_address: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Listening for RTMP connections on {}", listen_address);

    let listener = TcpListener::bind(listen_address).await?;

    loop {
        let (stream, connection_info) = listener.accept().await?;

        let connection = RtmpConnection {
            ip: connection_info.ip(),
            session: None
        };
        println!("Connection {}: Starting handshake", connection.ip);

        spawn(connection.handshake(stream));
    }
}

fn spawn<F, E>(future: F) where F: Future<Output = Result<(), E>> + Send + 'static, E: Display {
    tokio::task::spawn(async {
        match future.await {
            Err(err) => eprintln!("Encountered error while performing RTMP operation: {}", err),
            _ => {}
        };
    });
}

fn send<T>(sender: &UnboundedSender<T>, message: T) -> bool {
    match sender.send(message) {
        Ok(_) => true,
        Err(_) => false
    }
}
