mod app;
mod hdfs_sink;

pub fn main() {
    rlink::api::env::execute("rlink-files", crate::app::FilesStreamApp {});
}
