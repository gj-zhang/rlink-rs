pub trait Accumulator {
    fn add(&mut self, v: f64);
    fn get_local_value(&self) -> f64;
    fn reset_local_value(&mut self, v: f64);
    fn merge(&mut self, acc: Box<dyn Accumulator>);
}