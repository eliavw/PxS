# PxS
A `scikit-learn`-like interface for BayesFusion's `SMILE`.

## Introduction
BayesFusion's SMILE engine is one of the most highly respected packages for
Bayesian Modelling and offers a limited-in-time academic license. 

The package is written in `C/C++` and is closed-source, but headers are available.
This project is all about creating a convenient, `python`-based, front-end for the 
package, inspired by scikit-learn standards.

There's two approaches being tried here. One relies on command-line interaction
with custom made executables, where communication goes over disk. This is of
course slow and ugly, but also supposed to be very stable.

The other, more challeging approach was to design an actual C-binding to the
C++ library, and then use CTypes to interact with the resulting .so direclty.
This works to some extent, but is less stable at times. And, crucially, very
difficult to debug.
