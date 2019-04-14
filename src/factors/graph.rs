//! Graph Factors.
//! A Factor implementation for graph problems.

use std::ops::Mul;

use timely::dataflow::*;

use differential_dataflow::collection::Collection;
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;

use {Convert, Factor, Value};

pub struct GraphFactor<G: Scope, D: Monoid> {
    pub variables: Vec<u32>,
    pub tuples: Collection<G, Vec<Value>, D>,
}

impl<'a, G: Scope, D> Factor<'a, G, D> for GraphFactor<G, D>
where
    G::Timestamp: Lattice + Ord,
    D: Monoid + Mul<Output = D> + Convert,
{
    fn new(variables: Vec<u32>, tuples: Collection<G, Vec<Value>, D>) -> GraphFactor<G, D> {
        GraphFactor {
            variables: variables,
            tuples: tuples,
        }
    }
    fn normalize(
        tuples: Collection<G, (Vec<Value>, Vec<Value>), D>,
        pos: usize,
    ) -> Collection<G, Vec<Value>, D> {
        tuples
            .map(|(_k, v)| v.clone())
            .filter(|x| if x.len() == 2 { x[0] < x[1] } else { true })
    }
    fn variables(&self) -> Vec<u32> {
        self.variables.clone()
    }
    fn tuples(self) -> Collection<G, Vec<Value>, D> {
        self.tuples
    }
}
