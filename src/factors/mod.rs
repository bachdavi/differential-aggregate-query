//! Factors.

pub mod graph;

// Generic Factor implementation
use std::ops::Mul;

use timely::dataflow::*;

use differential_dataflow::collection::Collection;
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;

use {Convert, Factor, Value};

pub struct GenericFactor<G: Scope, D: Monoid> {
    pub variables: Vec<u32>,
    pub tuples: Collection<G, Vec<Value>, D>,
}

impl<'a, G: Scope, D> Factor<'a, G, D> for GenericFactor<G, D>
where
    G::Timestamp: Lattice + Ord,
    D: Monoid + Mul<Output = D> + Convert,
{
    fn new(variables: Vec<u32>, tuples: Collection<G, Vec<Value>, D>) -> GenericFactor<G, D> {
        GenericFactor {
            variables: variables,
            tuples: tuples,
        }
    }
    fn variables(&self) -> Vec<u32> {
        self.variables.clone()
    }
    fn tuples(self) -> Collection<G, Vec<Value>, D> {
        self.tuples
    }
}
