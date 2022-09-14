use crate::agg::accumulator::Accumulator;

pub struct SumAccumulator {
    local_value: f64,
}

impl SumAccumulator {
    pub fn new() -> Self {
        SumAccumulator {
            local_value: f64::default(),
        }
    }
}

impl Accumulator for SumAccumulator
    where f64: Default
{
    fn add(&mut self, v: f64) {
        self.local_value += v;
    }

    fn get_local_value(&self) -> f64 {
        self.local_value
    }

    fn reset_local_value(&mut self, v: f64) {
        self.local_value = v;
    }

    fn merge(&mut self, acc: Box<dyn Accumulator>) {
        self.local_value += acc.get_local_value();
    }
}
