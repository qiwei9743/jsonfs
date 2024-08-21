use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyWrite, Request, FUSE_ROOT_ID,
};
use libc::ENOENT;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

use std::fs;
use std::rc::Rc;

use serde_json::Value;
use std::path::{Path, PathBuf};

use slog::{error, info, o, warn, Drain, Logger};
use slog_async;
use slog_scope;
use slog_term;

use std::fs::OpenOptions;

mod tree;

struct JsonFS {
    json_path: Rc<PathBuf>,
    json: Rc<Value>,
    inodes: HashMap<u64, Rc<String>>,
    last_inode: u64,
}

fn get_json_at_path<'a, 'b>(json: &'b Value, path: &'a str) -> Option<&'b Value> {
    let mut current = json;
    for key in path.split('/').filter(|s| !s.is_empty()) {
        match current {
            Value::Object(map) => current = map.get(key)?,
            Value::Array(vec) => current = vec.get(key.parse::<usize>().ok()?)?,
            _ => return None,
        }
    }
    Some(current)
}

impl JsonFS {
    fn new(json_path: impl AsRef<Path>) -> Self {
        let data = fs::read_to_string(json_path.as_ref()).unwrap();
        let json = serde_json::from_str(&data).unwrap();

        let mut fs = JsonFS {
            json_path: Rc::new(json_path.as_ref().to_path_buf()),
            json: Rc::new(json),
            inodes: HashMap::new(),
            last_inode: FUSE_ROOT_ID,
        };
        fs.inodes.insert(FUSE_ROOT_ID, Rc::new("".to_string()));
        fs
    }

    fn allocate_inode(&mut self, path: String) -> u64 {
        self.last_inode += 1;
        self.inodes.insert(self.last_inode, Rc::new(path));
        self.last_inode
    }

    fn create_attr(&self, ino: u64, value: &Value) -> FileAttr {
        let kind = match value {
            Value::Object(_) | Value::Array(_) => FileType::Directory,
            _ => FileType::RegularFile,
        };

        let size = match value {
            Value::String(s) => s.len() as u64,
            _ => value.to_string().len() as u64,
        };

        FileAttr {
            ino,
            size,
            blocks: 1,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind,
            perm: 0o644,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    fn write_json_at_path(&mut self, path: &str, content: &str) {
        let mut current = Rc::make_mut(&mut self.json);
        for key in path.split('/').filter(|s| !s.is_empty()) {
            match current {
                Value::Object(map) => current = map.get_mut(key).unwrap(),
                Value::Array(vec) => current = vec.get_mut(key.parse::<usize>().unwrap()).unwrap(),
                _ => break,
            }
        }
        *current = Value::String(content.to_string());
    }
    fn write_json_at_path2(&mut self, path: &str, offset: i64, data: &str) {
        let mut current = Rc::make_mut(&mut self.json);

        // 查找路径
        for key in path.split('/').filter(|s| !s.is_empty()) {
            match current {
                Value::Object(map) => current = map.get_mut(key).unwrap(),
                Value::Array(vec) => current = vec.get_mut(key.parse::<usize>().unwrap()).unwrap(),
                _ => break,
            }
        }

        if let Value::String(ref mut original) = current {
            let mut new_content = original.clone();

            // 基于偏移量拼接新内容
            if offset >= 0 && (offset as usize) < original.len() {
                new_content.replace_range(offset as usize..offset as usize + data.len(), data);
            } else if offset as usize >= original.len() {
                new_content.push_str(data);
            } else {
                // 当offset不合法时，直接用data替换整个内容
                new_content = data.to_string();
            }

            *original = new_content;
        } else {
            println!("else current:{:?}", current);
        }
    }

    fn write_json_at_path3(&mut self, path: &str, offset: i64, data: &str) {
        let mut current = Rc::make_mut(&mut self.json);

        // 查找路径
        for key in path.split('/').filter(|s| !s.is_empty()) {
            match current {
                Value::Object(map) => current = map.get_mut(key).unwrap(),
                Value::Array(vec) => current = vec.get_mut(key.parse::<usize>().unwrap()).unwrap(),
                _ => break,
            }
        }

        match current {
            Value::String(ref mut original) => {
                let offset_usize = offset as usize;

                // 确保 offset 是合法的字符边界
                if original.is_char_boundary(offset_usize) {
                    if offset_usize >= original.len() {
                        // 如果 offset 超过当前字符串长度，追加数据
                        original.push_str(data);
                    } else {
                        // 如果 offset 在字符串范围内，替换数据
                        original.replace_range(offset_usize..offset_usize + data.len(), data);
                    }
                } else {
                    // 如果不是字符边界，可以选择返回错误或调整 offset
                    eprintln!("Offset is not a valid character boundary");
                }
            }
            Value::Number(num) => {
                // 将数字转换为字符串再处理
                let mut original = num.to_string();
                let offset_usize = offset as usize;

                if original.is_char_boundary(offset_usize) {
                    if offset_usize >= original.len() {
                        original.push_str(data);
                    } else {
                        original.replace_range(offset_usize..offset_usize + data.len(), data);
                    }
                    *current = Value::String(original);
                } else {
                    eprintln!("Offset is not a valid character boundary");
                }
            }
            _ => {
                // 对于其他类型，目前不支持写操作
            }
        }
    }

    fn myflush(&mut self) {
        eprintln!("Saving JSON data before unmounting...");
        let json_str = serde_json::to_string_pretty(self.json.as_ref()).unwrap();
        fs::write(self.json_path.as_ref(), json_str).unwrap();
        eprintln!("JSON data saved successfully.");
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct ReadDirReply<'a> {
    ino: u64,
    offset: u64,
    file_type: FileType,
    name: &'a str,
}

impl Filesystem for JsonFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        slog::debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "lookup", "io"=> "in", "parent" => parent, "name" => name.to_str().unwrap());
        let parent_path = self.inodes.get(&parent).unwrap();
        let path = format!("{}/{}", parent_path, name.to_str().unwrap());

        let json = Rc::clone(&self.json);

        if let Some(value) = get_json_at_path(json.as_ref(), &path) {
            let ino = self.allocate_inode(path);
            let attr = self.create_attr(ino, value);
            slog::debug!(slog_scope::logger(), "Filesystem func"; 
                "op" => "lookup", "io"=> "out", "attr" => format!("{:?}", attr));
            reply.entry(&Duration::new(1, 0), &attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        slog::debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "getattr", "io"=> "in", "ino" => ino);
        let json = Rc::clone(&self.json);
        if let Some(path) = self.inodes.get(&ino) {
            if let Some(value) = get_json_at_path(json.as_ref(), path) {
                let attr = self.create_attr(ino, value);
                slog::debug!(slog_scope::logger(), "Filesystem func"; 
                    "op" => "getattr", "io"=> "out", "attr" => format!("{:?}", attr));
                reply.attr(&Duration::new(1, 0), &attr);
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }
    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        slog::debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "read", "io"=> "in", 
            "ino" => ino, "fh" => _fh, "offset" => offset, "size" => size, 
            "flags" => _flags, "lock_owner" => _lock_owner);
        let json = Rc::clone(&self.json);
        if let Some(path) = self.inodes.get(&ino) {
            if let Some(value) = get_json_at_path(json.as_ref(), path) {
                let content = match value {
                    Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };

                let content_bytes = content.as_bytes();
                let start = offset as usize;
                let end = (offset as usize + size as usize).min(content_bytes.len());

                slog::debug!(slog_scope::logger(), "Filesystem func"; 
                    "op" => "read", "io"=> "out", "content" => &content[start..end]);

                reply.data(&content_bytes[start..end]);
            } else {
                reply.error(libc::ENOENT);
            }
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        slog::debug!(slog_scope::logger(), "Filesystem func";
            "op" => "readdir", "io"=> "in", "ino" => ino, "fh" => _fh, "offset" => offset);
        let json = Rc::clone(&self.json);
        let nums: Vec<String>;
        let mut reply_res: Vec<ReadDirReply> = vec![];
        if let Some(path) = self.inodes.get(&ino).map(|s| Rc::clone(s)) {
            if let Some(value) = get_json_at_path(json.as_ref(), path.as_str()) {
                let mut entries = vec![
                    (ino, FileType::Directory, "."),
                    (ino, FileType::Directory, ".."),
                ];

                match value {
                    Value::Object(map) => {
                        for (key, _) in map {
                            let child_ino = self.allocate_inode(format!("{}/{}", path, key));
                            entries.push((child_ino, FileType::RegularFile, key.as_str()));
                        }
                    }
                    Value::Array(vec) => {
                        nums = (0..vec.len()).map(|x| x.to_string()).collect();
                        for (index, _) in vec.iter().enumerate() {
                            let child_ino = self.allocate_inode(format!("{}/{}", path, index));
                            entries.push((child_ino, FileType::RegularFile, nums[index].as_str()));
                        }
                    }
                    _ => {}
                }

                for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                    let _ = reply.add(entry.0, (i + 1) as i64, entry.1, entry.2);
                    reply_res.push(ReadDirReply {
                        ino: entry.0,
                        offset: (i + 1) as u64,
                        file_type: entry.1,
                        name: entry.2,
                    })
                }
                slog::debug!(slog_scope::logger(), "Filesystem func";
                    "op" => "readdir", "io"=> "out", "reply" => format!("{:?}", reply_res));
            }
        }
        reply.ok();
    }

    //fn write(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, data: &[u8], _flags: i32, reply: ReplyWrite) {
    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        slog::debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "write", "io"=> "in", 
            "ino" => ino, "fh" => _fh, "offset" => _offset, "data" => format!("{:?}", data), 
            "flags" => _flags, "lock_owner" => _lock_owner);
        let path = Rc::clone(self.inodes.get(&ino).unwrap());
        let content = std::str::from_utf8(data).unwrap();

        //self.write_json_at_path3(path.as_str(), _offset,content);
        self.write_json_at_path(path.as_str(), content);
        reply.written(content.len() as u32);
        self.myflush();
    }
}

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
    setup_log();
    let _scope_guard = slog_scope::set_global_logger(setup_log());
    let json_file = std::env::args()
        .nth(1)
        .expect("Usage: hello_fuse <JSON_FILE>");
    let mountpoint = std::env::args()
        .nth(2)
        .expect("Usage: hello_fuse <MOUNTPOINT>");

    fuser::mount2(
        JsonFS::new(json_file),
        &mountpoint,
        &[MountOption::AutoUnmount, MountOption::AllowOther],
    )
    .unwrap();
    //fuser::spawn_mount2(JsonFS::new(json_file), &mountpoint, &[MountOption::AutoUnmount, MountOption::AllowOther]).unwrap();
}
