use fuser::MountOption;

use slog::{o, Drain, Logger};
use slog_async;
use slog_scope;
use slog_term;

use std::fs::OpenOptions;

// mod test;
// mod tree;
//mod jsonfs;
mod pinjsonfs;

fn setup_log() -> Logger {
    // 打开一个日志文件，支持追加模式
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("log.txt")
        .unwrap();

    // 创建一个终端风格的 Drain
    let decorator_term = slog_term::PlainDecorator::new(std::io::stdout());
    let drain_term = slog_term::FullFormat::new(decorator_term).build().fuse();
    let drain_term = slog_async::Async::new(drain_term).build().fuse();

    // 创建一个文件风格的 Drain
    let decorator_file = slog_term::PlainDecorator::new(file);
    let drain_file = slog_term::FullFormat::new(decorator_file).build().fuse();
    let drain_file = slog_async::Async::new(drain_file).build().fuse();

    // 合并两个 Drain
    let drain = slog::Duplicate::new(drain_term, drain_file).fuse();
    let drain = slog::LevelFilter::new(drain, slog::Level::Debug).fuse();

    // 创建一个 Logger 实例
    //Logger::root(drain, o!("version" => "1.0"))
    Logger::root(drain, o!())
}

fn main() {
    //env_logger::init();
    let _scope_guard = slog_scope::set_global_logger(setup_log());
    let json_file = std::env::args()
        .nth(1)
        .expect("Usage: hello_fuse <JSON_FILE>");
    let mountpoint = std::env::args()
        .nth(2)
        .expect("Usage: hello_fuse <MOUNTPOINT>");

    fuser::mount2(
        pinjsonfs::JsonFS::new(json_file),
        //jsonfs::JsonFS::new(json_file),
        &mountpoint,
        &[MountOption::AutoUnmount, MountOption::AllowOther],
    )
    .unwrap();
    //fuser::spawn_mount2(JsonFS::new(json_file), &mountpoint, &[MountOption::AutoUnmount, MountOption::AllowOther]).unwrap();
}
