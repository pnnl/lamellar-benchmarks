


use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;



pub struct Permutation{
    pub forward:    Vec<usize>,
    pub backward:   Vec<usize>,
}

impl Permutation {
    /// Returns the label assigned to an element
    pub fn get_forward( &self, original: usize ) -> usize { self.forward[original].clone() }

    /// Given a label, find the element it was attached to
    pub fn get_backward( &self, label: usize ) -> usize { self.backward[label].clone() }    

    /// Returns the label assigned to an element
    pub fn forward( &self ) -> &Vec<usize> { &self.forward }

    /// Given a label, find the element it was attached to
    pub fn backward( &self ) -> &Vec<usize> { &self.backward }   
    
    /// Generates a random permutation from a random seed
    pub fn random( length: usize, seed: usize ) -> Self {
        let mut rng = StdRng::seed_from_u64(seed as u64);
        let mut forward: Vec<_> = (0..length).collect(); 
        let mut backward    =   vec![0; forward.len()];       
        forward.shuffle(&mut rng); // Shuffle the elements to generate a random permutation
        for (original,label) in forward.iter().cloned().enumerate() {
            backward[label] = original;
        }
        return Permutation{ forward, backward }
    }
}