mod cli;
mod config;

#[tokio::main]
async fn main() {
    //todo clusterId use args

    //todo check cluster_name unique

    match cli::job_manager::run(config::k8s_config::Config::try_default()).await {
        Ok(()) => {
            println!(" cluster start successful");
        }
        Err(e) => {
            println!(" cluster start faild :{}", e);
        }
    }
}
