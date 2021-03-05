mod cli;
mod config;

#[tokio::main]
async fn main() {
    //todo clusterId use args

    //todo check cluster_name unique

 

    match cli::job_manager::run(config::k8s_config::Config::try_default(),String::new()).await{
        Ok(o)=>{
            println!(" cluster start successful :{}",o);
        }
        _=>{
            panic!("faild hahahahaha");
        }
    }
    
}
