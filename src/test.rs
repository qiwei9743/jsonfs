use std::marker::PhantomPinned;
use std::pin::Pin;

#[derive(Debug)]
struct Test {
    a: String,
    b: *const String,
    _marker: PhantomPinned,
}

impl Test {
    fn new(txt: &str) -> Pin<Box<Self>> {
        let t = Test {
            a: String::from(txt),
            b: std::ptr::null(),
            _marker: PhantomPinned,
        };
        let mut boxed = Box::pin(t);
        let self_ptr: *const String = &boxed.as_ref().a;
        unsafe { boxed.as_mut().get_unchecked_mut().b = self_ptr };

        boxed
    }

    fn a(self: Pin<&Self>) -> &str {
        &self.get_ref().a
    }

    fn b(self: Pin<&Self>) -> &String {
        unsafe { &*(self.b) }
    }
}

#[test]
fn test() {
    let t = Test::new("hello");

    assert_eq!(t.as_ref().a(), "hello");
    assert_eq!(t.as_ref().b(), &String::from("hello"));

    let t2 = t;
    assert_eq!(t2.as_ref().a(), "hello");
    assert_eq!(t2.as_ref().b(), &String::from("hello"));
}
