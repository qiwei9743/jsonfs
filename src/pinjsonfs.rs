use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyWrite, Request, FUSE_ROOT_ID,
};
use libc::ENOENT;
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;

use std::path::{Path, PathBuf};
use std::rc::Rc;

use std::time::{Duration, UNIX_EPOCH};

use slog::{debug, warn};

use std::marker::PhantomPinned;
use std::pin::Pin;

struct Inode {
    ino: u64,
    value: *mut Value,
}

pub(crate) struct JsonFS {
    json_path: Rc<PathBuf>,
    json: Value,
    ino2inode: HashMap<u64, Inode>,
    _marker: PhantomPinned,
}

impl JsonFS {
    pub(crate) fn new(json_path: impl AsRef<Path>) -> Pin<Box<JsonFS>> {
        let data = fs::read_to_string(json_path.as_ref()).unwrap();
        let json = serde_json::from_str(&data).unwrap();

        let fs = JsonFS {
            json_path: Rc::new(json_path.as_ref().to_path_buf()),
            json: json,
            ino2inode: HashMap::new(),
            _marker: PhantomPinned,
        };

        let mut fs = Box::pin(fs);

        let root = unsafe { &mut fs.as_mut().get_unchecked_mut().json as *mut Value };

        debug!(slog_scope::logger(), "Filesystem init"; "root" => root as u64);

        fs.as_mut().traverse(root);

        fs.as_mut().ino2inode_mut().insert(
            FUSE_ROOT_ID,
            Inode {
                ino: FUSE_ROOT_ID,
                value: root,
            },
        );

        fs
    }

    fn traverse(mut self: Pin<&mut JsonFS>, root: *mut Value) {
        let ino = root as u64;

        self.as_mut()
            .ino2inode_mut()
            .insert(ino, Inode { ino, value: root });

        let root_value: &mut Value;
        unsafe {
            root_value = &mut *root;
        }
        match root_value {
            Value::Object(map) => {
                for (_, value) in map.iter_mut() {
                    self.as_mut().traverse(value as *mut Value);
                }
            }
            Value::Array(vec) => {
                for value in vec.iter_mut() {
                    self.as_mut().traverse(value as *mut Value);
                }
            }
            _ => {}
        }
    }

    fn ino2inode_mut(self: Pin<&mut Self>) -> &mut HashMap<u64, Inode> {
        unsafe { &mut self.get_unchecked_mut().ino2inode }
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

    fn myflush(self: Pin<&mut Self>) {
        eprintln!("Saving JSON data flushing");
        let json_str = serde_json::to_string_pretty(&self.json).unwrap();
        fs::write(self.json_path.as_ref(), json_str).unwrap();
        eprintln!("JSON data saved successfully.");
    }
}

fn lookup_children<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => map.get(name),
        Value::Array(vec) => {
            debug!(slog_scope::logger(), "lookup_children"; "name" => name);
            vec.get(name.parse::<usize>().ok()?)
        }
        _ => None,
    }
}

fn get_value_type(value: &Value) -> FileType {
    match value {
        Value::Object(_) | Value::Array(_) => FileType::Directory,
        _ => FileType::RegularFile,
    }
}

impl Filesystem for Pin<Box<JsonFS>> {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "lookup", "io"=> "in", "parent" => parent, "name" => name.to_str().unwrap());

        if let Some(Inode { value, .. }) = self.ino2inode.get(&parent) {
            let parent_value = unsafe {
                let v2 = *value;
                &mut *v2
            };

            if let Some(value) = lookup_children(parent_value, name.to_str().unwrap()) {
                let child_ino = value as *const Value as u64;
                let attr = self.create_attr(child_ino, value);

                debug!(slog_scope::logger(), "Filesystem func"; 
                    "op" => "lookup", "io"=> "out", "attr" => format!("{:?}", attr));
                reply.entry(&Duration::new(1, 0), &attr, 0);
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "getattr", "io"=> "in", "ino" => ino);
        let json = unsafe { &mut self.as_mut().get_unchecked_mut().json as *mut Value };

        if let Some(Inode { value, ino }) = self.ino2inode.get(&ino) {
            let inov = *ino;
            let value = unsafe {
                let v1 = *value;
                debug!(slog_scope::logger(), "getattr"; "value" => v1 as u64, "ino" => inov, "json" => json as u64);

                &mut *v1
            };
            debug!(slog_scope::logger(), "getattr"; "value type" => format!("{:?}", get_value_type(value)));
            let attr = self.create_attr(inov, value);
            debug!(slog_scope::logger(), "Filesystem func"; 
                "op" => "getattr", "io"=> "out", "attr" => format!("{:?}", attr));
            reply.attr(&Duration::new(1, 0), &attr);
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
        debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "read", "io"=> "in", 
            "ino" => ino, "fh" => _fh, "offset" => offset, "size" => size, 
            "flags" => _flags, "lock_owner" => _lock_owner);

        if let Some(Inode { value, .. }) = self.ino2inode.get(&ino) {
            let value = unsafe {
                let v1 = *value;
                &mut *v1
            };
            match value {
                Value::Null => {
                    debug!(slog_scope::logger(), "Filesystem func";
                        "op" => "read", "io"=> "out", "content" => "null");
                    reply.data(&[]);
                    return;
                }
                Value::Bool(b) => {
                    debug!(slog_scope::logger(), "Filesystem func";
                        "op" => "read", "io"=> "out", "content" => format!("{}", b));
                    reply.data(&[if *b { 1 } else { 0 }]);
                    return;
                }
                Value::Number(n) => {
                    debug!(slog_scope::logger(), "Filesystem func";
                        "op" => "read", "io"=> "out", "content" => format!("{}", n));
                    reply.data(&n.to_string().as_bytes());
                    return;
                }
                Value::String(s) => {
                    let start = offset as usize;
                    let end = (offset as usize + size as usize).min(s.len());
                    debug!(slog_scope::logger(), "Filesystem func";
                    "op" => "read", "io"=> "out", "content" => s.as_str());
                    reply.data(&s.as_bytes()[start..end]);
                    return;
                }
                _ => (),
            }

            if let Value::String(s) = value {
                let content_bytes = s.as_bytes();
                let start = offset as usize;
                let end = (offset as usize + size as usize).min(content_bytes.len());

                debug!(slog_scope::logger(), "Filesystem func";
                    "op" => "read", "io"=> "out", "content" => &s[start..end]);

                reply.data(&content_bytes[start..end]);
                return;
            }
        }
        reply.error(libc::ENOENT);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!(slog_scope::logger(), "Filesystem func";
            "op" => "readdir", "io"=> "in", "ino" => ino, "fh" => _fh, "offset" => offset);

        if let Some(Inode { value, .. }) = self.ino2inode.get(&ino) {
            let value = unsafe {
                let v1 = *value;
                &mut *v1
            };
            match value {
                Value::Object(map) => {
                    let mut values: Vec<_> = map.iter().collect();
                    values.sort_by(|a, b| a.0.cmp(&b.0));

                    for (child_index, (child_key, child_value)) in
                        values.into_iter().enumerate().skip(offset as usize)
                    {
                        let child_ino = child_value as *const Value as u64;
                        let child_index = child_index + 1;
                        debug!(slog_scope::logger(), "Filesystem func map";
                            "op" => "readdir", "io"=> "out", "ino" => child_ino, "fh" => _fh, "offset" => offset, "child_key" => child_key,
                            "child_value" => format!("{:?}", get_value_type(child_value)), "child_index" => child_index);
                        let _ = reply.add(
                            child_ino,
                            child_index.try_into().unwrap(),
                            get_value_type(child_value),
                            child_key,
                        );
                    }
                }
                Value::Array(vec) => {
                    for (child_index, child_value) in vec.iter().enumerate().skip(offset as usize) {
                        let child_ino = child_value as *const Value as u64;
                        debug!(slog_scope::logger(), "Filesystem func vec";
                            "op" => "readdir", "io"=> "out", "ino" => child_ino, "fh" => _fh, "offset" => offset, "child_index" => child_index,
                            "child_value" => format!("{:?}", get_value_type(child_value)));
                        let _ = reply.add(
                            child_ino,
                            (child_index + 1).try_into().unwrap(),
                            get_value_type(child_value),
                            child_index.to_string(),
                        );
                    }
                }
                _ => {}
            }
        }
        reply.ok();
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        debug!(slog_scope::logger(), "Filesystem func";
            "op" => "mkdir", "io"=> "in", "parent" => parent, "name" => name.to_str().unwrap());

        if let Some(Inode { value, .. }) = self.ino2inode.get(&parent) {
            let parent = unsafe { value.as_mut().unwrap() };
            match parent {
                Value::Object(map) => {
                    map.iter()
                        .map(|(_, v)| v as *const Value as u64)
                        .for_each(|ino| {
                            self.as_mut().ino2inode_mut().remove(&ino);
                        });

                    let value = map
                        .entry(name.to_str().unwrap().to_string())
                        .or_insert(serde_json::json!({}));
                    let attr = self.create_attr(value as *mut Value as u64, value);
                    map.iter_mut().for_each(|(_, v)| {
                        let ino = v as *const Value as u64;
                        self.as_mut().ino2inode_mut().insert(
                            ino,
                            Inode {
                                ino,
                                value: v as *mut Value,
                            },
                        );
                    });
                    reply.entry(&Duration::new(1, 0), &attr, 0);
                }
                Value::Array(vec) => {
                    vec.iter()
                        .map(|v| v as *const Value as u64)
                        .for_each(|ino| {
                            self.as_mut().ino2inode_mut().remove(&ino);
                        });
                    vec.push(serde_json::json!({}));
                    let value = vec.last_mut().unwrap();
                    let attr = self.create_attr(value as *mut Value as u64, value);
                    vec.iter_mut().for_each(|v| {
                        let ino = v as *const Value as u64;
                        self.as_mut().ino2inode_mut().insert(
                            ino,
                            Inode {
                                ino,
                                value: v as *mut Value,
                            },
                        );
                    });
                    reply.entry(&Duration::new(1, 0), &attr, 0);
                }
                _ => {}
            }
        } else {
            warn!(slog_scope::logger(), "Filesystem func not found inode of parent"; "op" => "mkdir", "io"=> "in", "parent" => parent, "name" => name.to_str().unwrap());
            reply.error(libc::ENOENT);
            return;
        }
    }
    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let content = String::from_utf8_lossy(data).into_owned();
        let content_size = content.as_bytes().len();
        debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "write", "io"=> "in", 
            "ino" => ino, "fh" => _fh, "offset" => offset, "content" => format!("{:?}", content), 
            "flags" => _flags, "lock_owner" => _lock_owner, "data_size" => data.len(), "content_size" => content_size);

        if let Some(Inode { value, .. }) = self.ino2inode.get(&ino) {
            let value = unsafe {
                let v1 = *value;
                &mut *v1
            };
            if let Ok(content_num) = content.parse::<u64>().map(|n| n as usize) {
                *value = serde_json::json!(content_num);
            } else {
                match value {
                    Value::String(s) => {
                        s.replace_range(offset as usize.., &content);
                    }
                    _ => {
                        *value = serde_json::json!(content);
                    }
                }
            }
            reply.written(data.len() as u32);
            return;
        }
        reply.error(libc::ENOENT);
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "create", "io"=> "in", 
            "parent" => parent, "name" => name.to_str(), "mode" => mode, "umask" => umask, "flags" => flags);

        if let Some(Inode { value, .. }) = self.ino2inode.get(&parent) {
            let parent_value = unsafe {
                let v1 = *value;
                &mut *v1
            };
            match parent_value {
                Value::Object(map) => {
                    if map.len() == 0 && name.to_str().unwrap().parse::<u64>() == Ok(0) {
                        *parent_value = serde_json::json!([""]);
                        let child = parent_value.as_array_mut().unwrap().last_mut().unwrap();
                        let child_ino = child as *mut Value as u64;
                        self.as_mut().ino2inode_mut().insert(
                            child_ino,
                            Inode {
                                value: child as *mut Value,
                                ino: child_ino,
                            },
                        );
                        let attr = self.create_attr(child as *mut Value as u64, child);
                        reply.created(&Duration::new(1, 0), &attr, 0, 0, 0);

                        return;
                    }

                    map.iter()
                        .map(|(_, v)| v as *const Value as u64)
                        .for_each(|ino| {
                            self.as_mut().ino2inode_mut().remove(&ino);
                        });

                    let attr = {
                        let new_child_value = map
                            .entry(name.to_str().unwrap())
                            .or_insert(serde_json::json!(""));

                        let value_ptr = new_child_value as *mut Value;
                        let child_ino = value_ptr as u64;
                        self.create_attr(child_ino, &new_child_value)
                    };

                    map.iter_mut().for_each(|(_, v)| {
                        let ino = v as *const Value as u64;
                        self.as_mut().ino2inode_mut().insert(
                            ino,
                            Inode {
                                value: v as *mut Value,
                                ino,
                            },
                        );
                    });

                    debug!(slog_scope::logger(), "Filesystem func"; 
                        "op" => "create", "io"=> "out", "attr" => format!("{:?}", attr), "name" => name.to_str());
                    reply.created(&Duration::new(1, 0), &attr, 0, 0, 0);

                    return;
                }
                Value::Array(vec) => {
                    if vec.len() == 0 && name.to_str().unwrap().parse::<u64>() != Ok(0) {
                        *parent_value = serde_json::json!({name.to_str().unwrap():""});
                        let child = parent_value
                            .as_object_mut()
                            .unwrap()
                            .get_mut(name.to_str().unwrap())
                            .unwrap();
                        let child_ino = child as *mut Value as u64;
                        self.as_mut().ino2inode_mut().insert(
                            child_ino,
                            Inode {
                                value: child as *mut Value,
                                ino: child_ino,
                            },
                        );
                        let attr = self.create_attr(child_ino, child);
                        reply.created(&Duration::new(1, 0), &attr, 0, 0, 0);
                        return;
                    }

                    let index = name.to_str().unwrap().parse::<usize>();
                    if index.is_err() {
                        warn!(slog_scope::logger(), "Filesystem func"; 
                            "op" => "create", "io"=> "out", "error" => "invalid index", "name" => name.to_str());

                        reply.error(libc::EINVAL);
                        return;
                    }
                    let index = index.unwrap();
                    if index == vec.len() {
                        vec.iter()
                            .map(|v| v as *const Value as u64)
                            .for_each(|ino| {
                                self.as_mut().ino2inode_mut().remove(&ino);
                            });

                        vec.push(serde_json::json!(""));
                        let child_ptr = vec.last_mut().unwrap() as *mut Value;

                        let child_ino = child_ptr as u64;

                        let attr = self.create_attr(child_ino, vec.last().unwrap());

                        vec.iter_mut().for_each(|v| {
                            let v1 = v as *mut Value;
                            let ino = v1 as u64;
                            let value = v1;

                            self.as_mut()
                                .ino2inode_mut()
                                .insert(ino, Inode { ino, value });
                        });
                        debug!(slog_scope::logger(), "Filesystem func"; 
                            "op" => "create", "io"=> "out", "attr" => format!("{:?}", attr), "name" => name.to_str());
                        reply.created(&Duration::new(1, 0), &attr, 0, 0, 0);
                        return;
                    }
                }
                _ => {}
            }
        }
        reply.error(libc::ENOSYS);
    }
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "unlink", "io"=> "in", 
            "parent" => parent, "name" => name.to_str());

        if let Some(Inode { value, ino }) = self.ino2inode.get(&parent) {
            assert!(ino == &parent);
            let parent_value = unsafe {
                let v1 = *value;
                &mut *v1
            };
            match parent_value {
                Value::Object(map) => {
                    map.iter()
                        .map(|(_, v)| v as *const Value as u64)
                        .for_each(|ino| {
                            self.as_mut().ino2inode_mut().remove(&ino);
                        });

                    map.remove(name.to_str().unwrap());

                    map.iter_mut().for_each(|(_, v)| {
                        let ino = v as *const Value as u64;

                        self.as_mut().ino2inode_mut().insert(
                            ino,
                            Inode {
                                value: v as *mut Value,
                                ino,
                            },
                        );
                    });

                    self.as_mut().myflush();

                    reply.ok();
                    return;

                    /*                     if let Some(child_value) = map.get(name.to_str().unwrap()) {
                        let child_value_ptr = child_value as *const Value;
                        let child_ino = child_value_ptr as u64;
                        debug!(slog_scope::logger(), "Filesystem func";
                            "op" => "unlink", "io"=> "out",
                            "child_ino" => child_ino, "name" => name.to_str(), "parent" => parent);
                        self.as_mut().ino2inode_mut().remove(&child_ino);
                        map.remove(name.to_str().unwrap());

                        self.as_mut().myflush();

                        reply.ok();
                        return;
                    } */
                }
                Value::Array(vec) => {
                    if let Some(child_value) =
                        vec.get(name.to_str().unwrap().parse::<usize>().unwrap())
                    {
                        let child_value_ptr = child_value as *const Value;
                        let child_ino = child_value_ptr as u64;

                        self.as_mut().ino2inode_mut().remove(&child_ino);
                        vec.remove(name.to_str().unwrap().parse::<usize>().unwrap());
                        self.as_mut().myflush();

                        reply.ok();
                        return;
                    }
                }
                _ => {}
            }
        }

        reply.error(libc::ENOSYS);
    }
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "setattr", "io"=> "in", 
            "ino" => ino, "mode" => mode, "uid" => uid, "gid" => gid, "size" => size, 
            "fh" => fh, "flags" => flags);
        let attr = self.create_attr(ino, &Value::String("".to_string()));
        reply.attr(&Duration::new(1, 0), &attr);
    }
    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(slog_scope::logger(), "Filesystem func"; 
            "op" => "flush", "io"=> "in", 
            "fh" => _fh, "lock_owner" => _lock_owner);

        self.as_mut().myflush();

        reply.ok();
        debug!(slog_scope::logger(), "Filesystem func"; "op" => "flush", "io"=> "out");
    }
}

mod tests {
    use super::*;

    #[test]
    fn test() {}
}
