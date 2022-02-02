use std::borrow::Borrow;
use actix::{Actor, AsyncContext, StreamHandler};
use actix_web::{get, Error, HttpRequest, HttpResponse, web};
use actix_web_actors::ws;
use actix_web_actors::ws::CloseReason;

use serde;
use serde::{Deserialize, Serialize};

/// Websocket messages sent from the server to the nodes
#[derive(Serialize)]
#[serde(tag = "type")]
pub(crate) enum WebsocketServerMessage {
    StartRender(StartRenderMessage),
    EndRender(EndRenderMessage),
    Shutdown
}

#[derive(Serialize)]
pub(crate) struct StartRenderMessage {
    pub(crate) render_id: u64,

    pub(crate) width: u16,
    pub(crate) height: u16,
    pub(crate) fps: u16,
    pub(crate) frameblend: u16,
    pub(crate) shutter_angle: u16,

    pub(crate) video_codec: RenderVideoCodec,
    pub(crate) audio_codec: RenderAudioCodec,
    pub(crate) video_bitrate: u32,
    pub(crate) audio_bitrate: u32,

    pub(crate) video_quality: u8,
    pub(crate) audio_sample_rate: u32
}

#[derive(Serialize)]
pub(crate) struct EndRenderMessage {
    pub(crate) render_id: u64
}

#[derive(Serialize)]
#[serde(tag = "vcodec")]
pub(crate) enum RenderVideoCodec {
    H264, HEVC, VP8, VP9, DNXHD
}

#[derive(Serialize)]
#[serde(tag = "acodec")]
pub(crate) enum RenderAudioCodec {
    AAC, AC3, VORBIS, OPUS, FLAC
}

/// Websocket messages sent from the nodes to the server
#[derive(Deserialize)]
#[serde(tag = "type")]
pub(crate) enum WebsocketClientMessage {
    FailedRender(FailedRenderMessage),
    FinishedRender(FinishedRenderMessage)
}

#[derive(Deserialize)]
pub(crate) struct FailedRenderMessage {
    pub(crate) render_id: u64
}

#[derive(Deserialize)]
pub(crate) struct FinishedRenderMessage {
    pub(crate) render_id: u64
}

/// Implement handler for nodes' websocket connection to server
struct NodeWsHandler {
    ip: String
}

impl Actor for NodeWsHandler {
    type Context = ws::WebsocketContext<Self>;
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for NodeWsHandler {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                let res: serde_json::Result<WebsocketClientMessage> = serde_json::from_str(text.to_string().as_str());

                match res {
                    Ok(WebsocketClientMessage::FinishedRender(FinishedRenderMessage { render_id })) => {
                        println!("WS {}: Finished render operation {}", self.ip, render_id);
                    }
                    Ok(WebsocketClientMessage::FailedRender(FailedRenderMessage { render_id})) => {
                        eprintln!("WS {}: Failed render operation {}", self.ip, render_id);
                    }
                    Err(err) => {
                        eprintln!("WS {}: Failed to parse text message: {:?}", self.ip, err);
                    }
                }
            },
            Ok(ws::Message::Binary(_)) => {
                eprintln!("WS {}: Received unexpected binary message", self.ip);
            },
            Ok(ws::Message::Close(reason_option)) => {
                match reason_option {
                    Some(reason) => {
                        let close_code: u16 = reason.code.into();
                        eprintln!("WS {}: Closed connection to websocket node with code {} and reason {}",
                                  self.ip, close_code, reason.description.unwrap_or("N/A".to_string()));
                    }
                    None => {
                        eprintln!("WS {}: Closed connection to websocket node for unknown reason.", self.ip);
                    }
                }
            },
            Ok(_) => {
                // Ignore continuations, no-ops
            },
            Err(err) => {
                eprintln!("WS {}: Error receiving websocket message: {:?}", self.ip, err);
            }
        }
    }

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Opening websocket connection to {}", self.ip);
    }

    fn finished(&mut self, ctx: &mut Self::Context) {
        println!("Closing websocket connection to {}", self.ip);
    }
}

#[get("/node/")]
async fn websocket_endpoint(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    ws::start(NodeWsHandler { ip: req.connection_info().peer_addr().unwrap().to_string() }, &req, stream)
}

pub fn init(cfg: &mut web::ServiceConfig) {
    cfg.service(websocket_endpoint);
}
