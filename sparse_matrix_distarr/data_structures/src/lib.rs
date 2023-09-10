//! Lamellar spare matrix library.
//! 
//! Often we omit the structural nonzero coefficients from a sparse matrix, focusing only on its sparcity pattern.

pub mod distributed;
pub mod serial;
pub mod load;
pub mod bale;
pub mod binary_search;
