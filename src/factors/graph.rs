//! Graph Factors.
//! A Factor implementation for graph problems.

use timely::dataflow::*;

use differential_dataflow::collection::Collection;
use differential_dataflow::lattice::Lattice;

use {Aggregate, Factor};

pub struct GraphFactor<G: Scope> {
    pub vertices: Vec<u32>,
    pub tuples: Collection<G, Vec<u64>, isize>,
}

fn not(x: usize) -> usize {
    if x == 0 {
        1
    } else {
        0
    }
}

impl<'a, G: Scope> Factor<'a, G, Vec<u64>, Vec<u64>> for GraphFactor<G>
where
    G::Timestamp: Lattice + Ord,
{
    fn normalize(
        vertices: Vec<u32>,
        tuples: Collection<G, (Vec<u64>, (Vec<u64>, Vec<u64>)), isize>,
    ) -> GraphFactor<G> {
        GraphFactor {
            vertices: vertices,
            tuples: tuples
                // we need to check which value from the val_l and val_r we need to take
                .map(|(k, (val_l, val_r))| {
                    let pos_l: Vec<usize> = k
                        .iter()
                        .flat_map(|k| val_l.iter().position(|&v| v == *k))
                        .collect();
                    let pos_r: Vec<usize> = k
                        .iter()
                        .flat_map(|k| val_r.iter().position(|&v| v == *k))
                        .collect();
                    // if the joined key `k` has two values we simply take either both left or both right
                    // if not we take the negation of the position value: 0->1, 1->0
                    if pos_l.len() == 2 {
                        vec![val_l[0], val_l[1]]
                    } else {
                        vec![val_l[not(pos_l[0])], val_r[not(pos_r[0])]]
                    }
                })
                // only edges where the left is smaller than the right are valid
                .filter(|x| x[0] < x[1]),
        }
    }
    fn vertices(&self) -> Vec<u32> {
        self.vertices.clone()
    }
    fn tuples(self) -> Collection<G, Vec<u64>, isize> {
        self.tuples
    }
    fn participate(&self, var: &u32) -> bool {
        self.vertices.contains(&var)
    }
    fn tuples_by_variables(self, vars: &Vec<u32>) -> Collection<G, (Vec<u64>, Vec<u64>), isize> {
        // We join either left, right or both
        let pos: Vec<usize> = vars
            .into_iter()
            .flat_map(|x| self.vertices().iter().position(|&v| x == &v))
            .collect();
        self.tuples().map(move |tuple| {
            (
                tuple
                    .iter()
                    .enumerate()
                    .filter(|(i, _x)| pos.contains(&i))
                    .map(|(_i, x)| *x)
                    .collect(),
                tuple,
            )
        })
    }
}

/// Permitted aggregation function.
#[derive(Clone, Debug)]
pub enum AggregationFn {
    /// Sum
    SUM,
}

#[derive(Clone, Debug)]
pub struct GraphAggregate {
    pub aggregation_fn: AggregationFn,
}

// TODO Sort the graph factor aggregations out
impl<'a, G: Scope> Aggregate<'a, G, Vec<u64>, Vec<u64>, GraphFactor<G>> for GraphAggregate
where
    G::Timestamp: Lattice + Ord,
{
    fn implement(self, factor: GraphFactor<G>, var: u32) -> GraphFactor<G> {
        match self.aggregation_fn {
            AggregationFn::SUM => GraphFactor {
                vertices: factor
                    .vertices()
                    .iter()
                    .filter(|x| **x != var)
                    .cloned()
                    .collect(),
                tuples: factor.tuples(),
            },
        }
    }
}
