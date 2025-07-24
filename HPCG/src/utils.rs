use std::time::Instant;
use std::fmt;

pub struct Timing {
    label: String,
    start: Instant,
    end: Option<Instant>,
}

impl Timing {
    pub fn start(label: &str) -> Timing {
        Timing {
            label: label.to_string(),
            start: Instant::now(),
            end: Option::None
        }
    }

    pub fn end(self) -> Timing {
        Timing {
            label: self.label,
            start: self.start,
            end: Some(Instant::now())
        }
    }
}

impl fmt::Display for Timing {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //TODO: Cover the end-not-yet-done case
        let end = self.end.unwrap();
        let label = self.label.clone();
        let elapse = end.duration_since(self.start).as_secs_f64();

        write!(f, "{{\"label\": {label}, \"elapse_seconds\": {elapse} }}")
    }

}