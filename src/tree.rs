use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

pub(crate) struct Tree {
    root: Rc<RefCell<Inode>>,
    inodes: HashMap<u64, Rc<RefCell<Inode>>>, // inode_id => node
}

pub(crate) struct Inode {
    children: HashMap<String, Rc<RefCell<Inode>>>,
    component: String,
}

impl Inode {
    fn insert(&mut self, component: &str) -> &mut Rc<RefCell<Inode>> {
        self.children
            .entry(component.to_string())
            .or_insert_with(|| {
                let node = Rc::new(RefCell::new(Inode {
                    children: HashMap::new(),
                    component: component.to_string(),
                }));

                node
            })
    }
}

impl Tree {
    pub(crate) fn new(root: Rc<RefCell<Inode>>) -> Self {
        Tree {
            root,
            inodes: HashMap::new(),
        }
    }

    /*     pub(crate) fn get(&self, path: &str) -> Option<Rc<RefCell<Inode>>> {
        if path.is_empty() {
            return Some(self.root.clone());
        }

        let mut current = self.root.clone();
        for key in path.split('/').filter(|s| !s.is_empty()) {
            match current.borrow().children.get(key) {
                Some(child) => current = child.clone(),
                None => return None,
            }
        }
        Some(current)
    } */

    pub(crate) fn insert(&mut self, parent_ino: u64, component: &str) {} //-> Rc<RefCell<Inode>> {
                                                                         //self.inodes.get
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {}
}
