use std::time::Instant;
use std::fmt;
use std::cmp::max;

pub struct Timing {
    label: String,
    start: Instant,
    end: Option<Instant>,
    parts: Vec<Timing>
}

impl Timing {
    pub fn start(label: &str) -> Timing {
        Timing {
            label: label.to_string(),
            start: Instant::now(),
            end: Option::None,
            parts: Vec::new()
        }
    }

    pub fn end(self) -> Timing {
        Timing {
            label: self.label,
            start: self.start,
            end: Some(Instant::now()),
            parts: self.parts
        }
    }

    pub fn join(mut self, new_part: Timing) -> Timing {
        let end = {
            if self.end.is_some() && new_part.end.is_some() {
                Some(max(self.end.unwrap(), new_part.end.unwrap()))
            } else if self.end.is_some() {
                self.end
            } else {
                new_part.end
            }
        };
        
        self.parts.push(new_part);

        Timing {
            label: self.label,
            start: self.start,
            end: end,
            parts: self.parts
        }
    }

}

impl fmt::Display for Timing {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //TODO: Cover the end-not-yet-done case
        //TODO: Emit parts as a list json element
        let end = self.end.unwrap();
        let label = self.label.clone();
        let elapse = end.duration_since(self.start).as_secs_f64();

        write!(f, "{{\"label\": {label}, \"elapse_seconds\": {elapse} }}")
    }

}