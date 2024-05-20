



// !!!!!!! DRAFT; NOT WORKING
//
// The purpose of this file is to explore how to enable a PE to send out a request that all other PE's provide a piece of information


fn main() {
    // let mut v                   =   vec![ 0.0; world.my_pe ];


    // // wrap in LocalRwDarc's
    // let v                       =   LocalRwDarc::new( world.team(), v        ).unwrap();


    
}




// /// Allows each node to send information to another node
// #[lamellar::AmData(Debug, Clone)]
// pub struct SendV {
//     pub v_fetcher:              LocalRwDarc< Vec< usize > >,
//     pub v_sender:               Vec< usize >
// }

// #[lamellar::am]
// impl LamellarAM for RelaxAM {
//     async fn exec(self) {        
//         let mut v_fetcher         =   self.v_fetcher.write().await; // get a writable handle on the local ladle
//         v_fetcher.append( & mut self.v_sender );
//     }
// }


// /// Allows each node request information from another node
// #[lamellar::AmData(Debug, Clone)]
// pub struct FetchV {
//     pub pe_fetcher:             usize,
//     pub v_sender:               LocalRwDarc< Vec< usize > >
// }

// #[lamellar::am]
// impl LamellarAM for RelaxAM {
//     async fn exec(self) {    
//         // HAVEN'T FIGURE OUT HOW TO FILL THIS IN
//     }
// }