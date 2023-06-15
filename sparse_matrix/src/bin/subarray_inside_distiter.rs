//! Test whether a subarray created inside a distiter only has access to the data on the PE that called the subarray's creation.

use data_structures::*;
use data_structures::distributed::*;
use data_structures::bale::sparsemat as bale;
use tabled::{Table, Tabled};

use lamellar::{LamellarWorld, LamellarWorldBuilder};  
use lamellar::ActiveMessaging;



fn main() {
    
    // the world
    let world = LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();

    test_subarray_inside_dist_iter( &world );
}