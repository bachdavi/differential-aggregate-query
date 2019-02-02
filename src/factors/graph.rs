//! Graph Factors.
//! A Factor implementation for graph problems.

use timely::dataflow::*;

use differential_dataflow::collection::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Consolidate;

use {Aggregate, Factor, Value};

pub struct GraphFactor<G: Scope> {
    pub vertices: Vec<u32>,
    pub tuples: Collection<G, Vec<Value>, isize>,
}

impl<'a, G: Scope> Factor<'a, G> for GraphFactor<G>
where
    G::Timestamp: Lattice + Ord,
{
    fn normalize(
        vertices: Vec<u32>,
        tuples: Collection<G, (Vec<Value>, Vec<Value>), isize>,
    ) -> GraphFactor<G> {
        GraphFactor {
            vertices: vertices,
            tuples: tuples
                .map(|(_k, v)| v.clone())
                .filter(|x| if x.len() > 2 { x[0] < x[1] } else { true })
                .consolidate(),
        }
    }
    fn vertices(&self) -> Vec<u32> {
        self.vertices.clone()
    }
    fn tuples(self) -> Collection<G, Vec<Value>, isize> {
        self.tuples
    }
    fn participate(&self, var: &u32) -> bool {
        self.vertices.contains(&var)
    }
    fn tuples_by_variables(
        self,
        vars: &Vec<u32>,
    ) -> Collection<G, (Vec<Value>, Vec<Value>), isize> {
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
                    .map(|(_i, x)| x.clone())
                    .collect(),
                tuple
                    .iter()
                    .enumerate()
                    .filter(|(i, _x)| !pos.contains(&i))
                    .map(|(_i, x)| x.clone())
                    .collect(),
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
impl<'a, G: Scope> Aggregate<'a, G, GraphFactor<G>> for GraphAggregate
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
