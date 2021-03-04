mod cli;
mod config;

#[tokio::main]
async fn main() {
    //todo clusterId use args
    match cli::job_manager::run(config::k8s_config::JobConfig::default(),String::new()).await{
        Ok(o)=>{
            println!(" cluster start successful :{}",o);
        }
        _=>{
            panic!("faild hahahahaha");
        }
    }
    
}
