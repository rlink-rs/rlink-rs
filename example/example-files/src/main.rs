mod app;
mod hdfs_sink;

pub fn main() {
    rlink::core::env::execute(crate::app::FilesStreamApp {});
}
