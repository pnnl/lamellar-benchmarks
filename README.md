Benchmarks for the Lamellar Runtime
=================================================

Lamellar is an asynchronous tasking runtime for HPC systems developed in RUST

SUMMARY
-------

A collection of benchmarks to test the functionality and performance of the Lamellar runtime (https://github.com/pnnl/lamellar-runtime)

NEWS
----
Feb 2020: First alpha release

BUILD REQUIREMENTS
------------------
These benchmarks requires the following dependencies:

* [Lamellar](https://gitlab.pnnl.gov/lamellar/lamellar-runtime)
At the time of release, Lamellar has been tested with the following external packages:

| **GCC** | **CLANG** | **ROFI**  | **OFI**   | **IB VERBS**  | **MPI**       | **SLURM** | **LAMELLAR** |
|--------:|----------:|----------:|----------:|--------------:|--------------:|----------:|-------------:|
| 7.1.0   | 8.0.1     | 0.1.0     | 1.9.0     | 1.13          | mvapich2/2.3a | 17.02.7   | 0.1.0        |

The OFI_DIR environment variable must be specified with the location of the OFI installation.
The ROFI_DIR environment variable must be specified with the location of the ROFI installation.

BUILDING PACKAGE
----------------
In the following, assume a root directory ${ROOT}
0. download Benchmarks to ${ROOT}/Benchmarks 
    `cd ${ROOT} && git clone https://github.com/pnnl/lamellar-benchmarks`
0. download Lamellar to ${ROOT}/lamellar-runtime  -- or update Cargo.toml to point to the proper location
    `cd ${ROOT} && git clone https://github.com/pnnl/lamellar-runtime`

1. see readmes in  "histo", "triangle_count"


HISTORY
-------
- version 0.1:
  - histo
  - triangle count
  
NOTES
-----

STATUS
------
Working on additional benchmarks

CONTACTS
--------
Ryan Friese     - ryan.friese@pnnl.gov  
Roberto Gioiosa - roberto.gioiosa@pnnl.gov  
Mark Raugas     - mark.raugas@pnnl.gov  

## License

This project is licensed under the BSD License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

This work was supported by the High Performance Data Analytics (HPDA) Program at Pacific Northwest National Laboratory (PNNL),
a multi-program DOE laboratory operated by Battelle.
