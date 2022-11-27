//! Lamellar spare matrix library.
//! 
//! Often we omit the structural nonzero coefficients from a sparse matrix, focusing only on its sparcity pattern.

pub mod bale_original_spmat_serial;
pub mod bale_original_err;

pub mod lamellar_spmat_distributed;
pub mod lamellar_spmat_serial;

