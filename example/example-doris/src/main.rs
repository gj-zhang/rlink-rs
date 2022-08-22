mod app;

#[macro_use]
extern crate log;

fn main() {
    rlink::core::env::execute(crate::app::DorisApp {});
}
