# [rlink-rs](https://rlink.rs)

[![Crates.io](https://img.shields.io/crates/v/rlink?color=blue)](https://crates.io/crates/rlink)
[![Released API docs](https://docs.rs/rlink/badge.svg)](https://docs.rs/rlink-core)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE-MIT)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE-APACHE)

High performance Stream Processing Framework. A new, faster, implementation of Apache Flink from scratch in Rust. 
pure memory, zero copy. single cluster in the production environment stable hundreds of millions per second window calculation.

Framework tested on Linux/MacOS/Windows, requires stable Rust.

## Build
#### Build source
```bash
# debug
cargo build --color=always --all --all-targets
# release
cargo build --release --color=always --all --all-targets
```

## Standalone Deploy
### Config
#### standalone.yaml
```bash
# all job manager's addresses, one or more
application_manager_address: ["http://x.x.x.x:8370","http://y.y.y.y:8370"]

metadata_storage_mode: "memory"
metadata_storage_endpoints: []

# bind ip
task_manager_bind_ip: "z.z.z.z"
task_manager_work_dir: "/xxx/rlink/job"
```
#### task_managers
TaskManager list
```bash
10.1.2.1
10.1.2.2
10.1.2.3
10.1.2.4
```

### Launch
Coordinator
```bash
./start_job_manager.sh
```

Worker
```bash
./start_task_manager.sh
```

### Submit task 

#### On Standalone
```bash
## job demo

# create job
curl http://x.x.x.x:8370/job/application -X POST -F "file=@/path/to/execute_file" -v

# run job
curl http://x.x.x.x:8370/job/application/application-1591174445599 -X POST -H "Content-Type:application/json" -d '{"batch_args":[{"cluster_mode":"Standalone", "manager_type":"Coordinator","num_task_managers":"15","source_parallelism":"30", "reduce_parallelism":"30", "env":"dev"}]}' -v

# kill job
curl http://x.x.x.x:8370/job/application/application-1591174445599/shutdown -X POST -H "Content-Type:application/json"
```

## On Yarn
// todo
