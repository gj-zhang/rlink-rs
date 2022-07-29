#[macro_use]
extern crate rlink_derive;

mod app;

fn main() {
    rlink::core::env::execute(crate::app::DorisApp {});
}
