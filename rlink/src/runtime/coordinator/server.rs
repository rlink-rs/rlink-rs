use std::sync::{Arc, Mutex};

use actix_web::http::header;
use actix_web::web::Data;
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};
use rand::prelude::*;

use crate::api::checkpoint::Checkpoint;
use crate::api::cluster::MetadataStorageType;
use crate::api::cluster::{ResponseCode, StdResponse};
use crate::dag::metadata::DagMetadata;
use crate::runtime::coordinator::checkpoint_manager::CheckpointManager;
use crate::runtime::TaskManagerStatus;
use crate::storage::metadata::MetadataStorage;
use crate::storage::metadata::TMetadataStorage;
use crate::utils::VERSION;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WebContext {
    job_context: crate::runtime::context::Context,
    metadata_mode: MetadataStorageType,
}

pub(crate) fn web_launch(
    context: crate::runtime::context::Context,
    metadata_mode: MetadataStorageType,
    checkpoint_manager: CheckpointManager,
    dag_metadata: DagMetadata,
) -> String {
    let address: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let address_clone = address.clone();
    std::thread::Builder::new()
        .name("WebUI".to_string())
        .spawn(move || {
            serve_sync(
                context,
                metadata_mode,
                address_clone,
                checkpoint_manager,
                dag_metadata,
            );
        })
        .unwrap();

    loop {
        std::thread::sleep(std::time::Duration::from_millis(100));

        let address = address.lock().unwrap();
        if let Some(add) = &*address {
            return add.clone();
        }
    }
}

pub(crate) fn serve_sync(
    job_context: crate::runtime::context::Context,
    metadata_mode: MetadataStorageType,
    address: Arc<Mutex<Option<String>>>,
    checkpoint_manager: CheckpointManager,
    dag_metadata: DagMetadata,
) {
    actix_rt::System::new("Coordinator Web UI")
        .block_on(serve(
            job_context,
            metadata_mode,
            address,
            checkpoint_manager,
            dag_metadata,
        ))
        .unwrap();
}

async fn serve(
    job_context: crate::runtime::context::Context,
    metadata_mode: MetadataStorageType,
    rt_address: Arc<Mutex<Option<String>>>,
    checkpoint_manager: CheckpointManager,
    dag_metadata: DagMetadata,
) -> std::io::Result<()> {
    let context = WebContext {
        job_context,
        metadata_mode,
    };

    let ip = context.job_context.bind_ip.clone();

    let mut rng = rand::thread_rng();
    for _ in 0..30 {
        let port = rng.gen_range(10000, 30000);
        let address = format!("{}:{}", ip.as_str(), port);

        let data = Data::new(context.clone());
        let data_ck_manager = Data::new(checkpoint_manager.clone());
        let dag_metadata = Data::new(dag_metadata.clone());
        let server = HttpServer::new(move || {
            App::new()
                .app_data(data.clone())
                .app_data(data_ck_manager.clone())
                .app_data(dag_metadata.clone())
                .wrap(middleware::Logger::default())
                .wrap(middleware::DefaultHeaders::new().header("X-Version", VERSION))
                .service(
                    web::resource("/")
                        .wrap(
                            middleware::DefaultHeaders::new()
                                .header(header::CONTENT_TYPE, "text/html; charset=UTF-8"),
                        )
                        .route(web::get().to(index)),
                )
                .service(
                    web::resource("/dag/stream_graph.html")
                        .wrap(
                            middleware::DefaultHeaders::new()
                                .header(header::CONTENT_TYPE, "text/html; charset=UTF-8"),
                        )
                        .route(web::get().to(get_stream_graph_page)),
                )
                .service(
                    web::resource("/dag/execution_graph.html")
                        .wrap(
                            middleware::DefaultHeaders::new()
                                .header(header::CONTENT_TYPE, "text/html; charset=UTF-8"),
                        )
                        .route(web::get().to(get_execution_graph_page)),
                )
                .service(
                    web::resource("/dag/job_graph.html")
                        .wrap(
                            middleware::DefaultHeaders::new()
                                .header(header::CONTENT_TYPE, "text/html; charset=UTF-8"),
                        )
                        .route(web::get().to(get_job_graph_page)),
                )
                .service(web::resource("/heartbeat").route(web::post().to(heartbeat)))
                .service(web::resource("/context").route(web::get().to(get_context)))
                .service(web::resource("/metadata").route(web::get().to(get_metadata)))
                .service(web::resource("/checkpoint").route(web::post().to(register_checkpoint)))
                .service(web::resource("/checkpoints").route(web::get().to(get_checkpoint)))
                .service(web::resource("/dag/stream_graph").route(web::get().to(get_stream_graph)))
                .service(web::resource("/dag/job_graph").route(web::get().to(get_job_graph)))
                .service(
                    web::resource("/dag/execution_graph").route(web::get().to(get_execution_graph)),
                )
            // .service(web::resource("/dag/physic_graph").route(web::get().to(get_physic_graph)))
        })
        .disable_signals()
        .workers(8)
        .bind(address.clone());

        match server {
            Ok(x) => {
                {
                    let mut rt_address = rt_address.lock().unwrap();
                    *rt_address = Some(format!("http://{}", address));
                }
                return x.run().await;
            }
            Err(_e) => {
                //ignore
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "port inuse",
    ))
}

fn index() -> HttpResponse {
    let html = r#"<html>
        <head><title>rlink-rs Home</title></head>
        <body>
            <h1>rlink api</h1>
            <ul>
                <li><a href="context">context</a></li>
                <li><a href="metadata">metadata</a></li>
                <li><a href="checkpoints">checkpoints</a></li>
                <li><a href="dag/stream_graph">dag:stream_graph</a></li>
                <li><a href="dag/job_graph">dag:job_graph</a></li>
                <li><a href="dag/execution_graph">dag:execution_graph</a></li>
            </ul>
            
            <h1>rlink page</h1>
            <ul>
                <li><a href="dag/stream_graph.html">dag:stream_graph</a></li>
                <li><a href="dag/job_graph.html">dag:job_graph</a></li>
                <li><a href="dag/execution_graph.html">dag:execution_graph</a></li>
            </ul>
        </body>
    </html>"#;

    HttpResponse::Ok().body(html)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct HeartbeatModel {
    pub task_manager_id: String,
    pub task_manager_address: String,
    pub metrics_address: String,
    // ok, panic
    pub status: String,
}

pub(crate) async fn heartbeat(
    heartbeat_model: web::Json<HeartbeatModel>,
    context: Data<WebContext>,
) -> Result<HttpResponse, Error> {
    let metadata_storage = MetadataStorage::new(&context.metadata_mode);

    if !heartbeat_model.status.eq("ok") {
        error!(
            "heartbeat status: {}, model: {:?} ",
            heartbeat_model.status.as_str(),
            heartbeat_model
        );
    }

    metadata_storage
        .update_task_manager_status(
            heartbeat_model.task_manager_id.as_str(),
            heartbeat_model.task_manager_address.as_str(),
            TaskManagerStatus::Registered,
            heartbeat_model.metrics_address.as_str(),
        )
        .unwrap();

    let response = StdResponse::new(ResponseCode::OK, Some(true));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_context(context: Data<WebContext>) -> Result<HttpResponse, Error> {
    let response = StdResponse::new(ResponseCode::OK, Some(context.job_context.clone()));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_metadata(context: Data<WebContext>) -> Result<HttpResponse, Error> {
    let metadata_storage = MetadataStorage::new(&context.metadata_mode);
    let job_descriptor = metadata_storage.load().unwrap();

    let response = StdResponse::new(ResponseCode::OK, Some(job_descriptor));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn register_checkpoint(
    ck_model: web::Json<Checkpoint>,
    ck_manager: Data<CheckpointManager>,
) -> Result<HttpResponse, Error> {
    debug!(
        "<<<<<< register checkpoint to coordinator. {:?}",
        &ck_model.0
    );
    let resp = match ck_manager.get_ref().add(ck_model.0) {
        Ok(_) => "ok",
        Err(e) => {
            error!("register checkpoint error. {}", e);
            "error"
        }
    };

    let response = StdResponse::new(ResponseCode::OK, Some(resp.to_string()));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_checkpoint(
    ck_manager: Data<CheckpointManager>,
) -> Result<HttpResponse, Error> {
    let cks = ck_manager.get_ref().get();

    let response = StdResponse::new(ResponseCode::OK, Some(cks));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_stream_graph(
    dag_metadata: Data<DagMetadata>,
) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().stream_graph().clone();

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_job_graph(dag_metadata: Data<DagMetadata>) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().job_graph().clone();

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_execution_graph(
    dag_metadata: Data<DagMetadata>,
) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().execution_graph().clone();

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

// pub(crate) async fn get_physic_graph(dag_metadata: Data<DagMetadata>) -> Result<HttpResponse, Error> {
//     let task_groups = &dag_metadata.get_ref().physic_graph().task_groups.clone();
//
//     let response = StdResponse::new(ResponseCode::OK, Some(task_groups));
//     Ok(HttpResponse::Ok().json(response))
// }

async fn get_stream_graph_page(dag_metadata: Data<DagMetadata>) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().stream_graph();

    let json = serde_json::to_string(json_dag).unwrap();
    let html = DAG_TEMPLATE.replace("DAG_PLACE_HOLDER", json.as_str());

    Ok(HttpResponse::Ok().body(html))
}

async fn get_job_graph_page(dag_metadata: Data<DagMetadata>) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().job_graph();

    let json = serde_json::to_string(&json_dag).unwrap();
    let html = DAG_TEMPLATE.replace("DAG_PLACE_HOLDER", json.as_str());

    Ok(HttpResponse::Ok().body(html))
}

async fn get_execution_graph_page(dag_metadata: Data<DagMetadata>) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().execution_graph();

    let json = serde_json::to_string(&json_dag).unwrap();
    let html = DAG_TEMPLATE.replace("DAG_PLACE_HOLDER", json.as_str());

    Ok(HttpResponse::Ok().body(html))
}

const DAG_TEMPLATE: &'static str = r##"
<!DOCTYPE html>
<html lang="en">
<title>rlink-rs DAG</title>
<head>
<script src="https://d3js.org/d3.v3.min.js" charset="utf-8"></script>
<script src="https://cdn.bootcss.com/dagre-d3/0.6.4/dagre-d3.js"></script>
<!--<script src="https://dagrejs.github.io/project/dagre-d3/v0.6.4/dagre-d3.min.js"></script>-->
<style>
html,
body {
    width: 100%;
    height: 100%;
    position: relative;
    margin: 0;
    padding: 0;
    background: #3e3e3e;
}
#tree {
    width: 100%;
    height: 100%;
    display: flex;
    position: relative;
}
#tree svg {
    width: 100%;
    height: 100%;
}
text {
    font-size: 14px;
    fill: #fff;
}
.edgePath path {
    stroke: #d9822b;
    fill: #d9822b;
    stroke-width: 1.5px;
}
.node circle {
    fill: #000000;
}
/* tree svg */

.chartTooltip {
    position: absolute;
    height: auto;
    padding: 10px;
    box-sizing: border-box;
    background-color: white;
    border-radius: 5px;
    box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.4);
    opacity: 0;
}
.chartTooltip p {
    margin: 0;
    font-size: 14px;
    line-height: 20px;
    word-wrap: break-word;
}
.chartTooltip p span {
    display: flex;
}
.chartTooltip p a {
    display: flex;
}
.author {
    position: absolute;
    top: 20px;
    left: 20px;
}
</style>
</head>

<div id="tree">
    <div class="chartTooltip">
        <p id="chartTooltipText">
            <span class="chartTooltip-label"></span>
            <span class="chartTooltip-name"></span>
        </p>
    </div>
</div>

<script type="text/javascript">
let width = document.getElementById("tree").offsetWidth;
let height = document.getElementById("tree").offsetHeight;
// Create a new directed graph
let g = new dagreD3.graphlib.Graph().setGraph({
    rankdir: 'TB',
    edgesep: 100,
    ranksep: 80
});

let dag = DAG_PLACE_HOLDER;

let states = dag.nodes.forEach(function(node){
    let node_id = node.id;
    let value = {
        shape: "rect",
        name: node.name,
        label : node.label,
        rx: 5,
        ry: 5,
    }
    
    g.setNode(node_id, value);
});

dag.edges.forEach(function(edge) {
    let source_node_id = edge.source;
    let target_node_id = edge.target;
    let label = edge.label;
    
    g.setEdge(source_node_id, target_node_id, {
        label: label,
        lineInterpolate: 'basis',
        style: "fill: none; stroke: #d9822b"
    });
});

let render = new dagreD3.render();

// Set up an SVG group so that we can translate the final graph.
let svg = d3.select("#tree").append('svg');
let inner = svg.append("g");
render(inner, g);
// Set up zoom support
let zoom = d3.behavior.zoom().scaleExtent([0.1, 100])
    .on('zoomstart', () => {
        svg.style('cursor', 'move')
    })
    .on("zoom", function() {
        inner.attr('transform',
            "translate(" + d3.event.translate + ")" +
            "scale(" + d3.event.scale + ")"
        )
    }).on('zoomend', () => {
        svg.style('cursor', 'default')
    });
svg.call(zoom);

let timer;
const nodeEnter = inner.selectAll('g.node');
// 圆点添加 提示框
nodeEnter
    .on('mouseover', function(d) {
        tooltipOver(d)
        console.log(d)
    })
    .on('mouseout', () => {
        timer = setTimeout(function() {
            d3.select('.chartTooltip').transition().duration(300).style('opacity', 0).style('display', 'none')
        }, 200)
    });

// 偏移节点内文本内容
// nodeEnter.select('g.label').attr('transform', 'translate(0, 0)');
// 添加 tag 标签
// nodeEnter.append('text')
//     .text((d) => {
//         return d
//     });

function tooltipOver(d) {
    if (timer) clearTimeout(timer);
    d3.select('.chartTooltip').transition().duration(300).style('opacity', 1).style('display', 'block');
    const yPosition = d3.event.layerY + 20;
    const xPosition = d3.event.layerX + 20;
    const chartTooltip = d3.select('.chartTooltip')
        .style('left', xPosition + 'px')
        .style('top', yPosition + 'px');
    
    d3.select('.chartTooltip').on('mouseover', () => {
        if (timer) clearTimeout(timer);
        d3.select('.chartTooltip').transition().duration(300).style('opacity', 1).style('display', 'block')
    }).on('mouseout', () => {
        timer = setTimeout(function() {
            d3.select('.chartTooltip').transition().duration(300).style('opacity', 0).style('display', 'none')
        }, 200)
    });
    
    if (d) {
        chartTooltip.select('.chartTooltip-label').text('label：' + d)
    } else {
        chartTooltip.select('.chartTooltip-label').text('label：' + d)
    }
    if (g.node(d).name) {
        chartTooltip.select('.chartTooltip-name').text('名字2：' + g.node(d).name)
    } else {
        chartTooltip.select('.chartTooltip-name').text('名字2：' + g.node(d).name)
    }
}
</script>
</html>"##;
