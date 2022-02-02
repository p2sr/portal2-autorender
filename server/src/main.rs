use actix_web::{App, HttpServer};

mod rtmp_connection;
mod nodes;

const HTTP_LISTEN_ADDRESS: &str = "0.0.0.0:8080";
const RTMP_LISTEN_ADDRESS: &str = "0.0.0.0:1935";

// sv_cheats 1
// host_framerate 60
// sar_render_start "rtmp://localhost:1935/render.flv"

#[actix_web::main]
async fn main() -> serenity::futures::io::Result<()>{
    println!("Spawning RTMP Server");
    tokio::task::spawn(async {
        match rtmp_connection::run_rtmp_server(RTMP_LISTEN_ADDRESS).await {
            Err(err) => eprintln!("RTMP server failed with error: {}", err),
            _ => {}
        }
    });

    println!("Spawning HTTP Server");
    HttpServer::new(move || { App::new().configure(nodes::ws::init) })
        .bind(HTTP_LISTEN_ADDRESS)?
        .run()
        .await
}
