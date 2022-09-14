mod app;
mod function;
mod buffer_gen;
mod agg;

#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;

extern crate anyhow;

// mod buffer_gen {
//     include!(concat!(env!("OUT_DIR"), "/buffer_gen2/mod.rs"));
// }

fn main() {
    rlink::core::env::execute(app::AlarmPlatformDemo::new());
}