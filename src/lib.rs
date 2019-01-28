//! Core infrastructure
//!
//! This crate contains types and logic for Functional Aggregate Queries (FAQ).
//! Every factor in the FAQ query consists of vertices and
//! resembles one hyperedge in the queries hypergraph.
//! We represent every factor as one differential collection.
//! Computing the FAQ query is an iterative elimination of all bound
//! variables.

#![feature(drain_filter)]
extern crate differential_dataflow;
extern crate timely;

use timely::dataflow::*;

use differential_dataflow::collection::Collection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Join as JoinMap;
use differential_dataflow::Data;

pub mod factors;

// Union of two vectors preserving the order
pub fn vertex_union<T: PartialEq + Clone>(left: &Vec<T>, right: &Vec<T>) -> Vec<T> {
    left.iter()
        .filter(|x| !right.contains(x))
        .chain(right.iter())
        .cloned()
        .collect()
}

pub trait Factor<'a, G: Scope, D, K>
where
    D: Data,
    K: Data + Hashable,
    G::Timestamp: Lattice + Ord,
{
    /// Creates a new factor from the joined collection
    fn normalize(vertices: Vec<u32>, tuples: Collection<G, (K, (D, D)), isize>) -> Self;
    /// List the vertices for a given factor
    fn vertices(&self) -> Vec<u32>;
    /// A collection containing all tuples
    fn tuples(self) -> Collection<G, D, isize>;
    /// Determine if the given factor should participate in insideOut
    fn participate(&self, var: &u32) -> bool;
    /// Arrange the tuples, s.t. we can join
    fn tuples_by_variables(self, vars: &Vec<u32>) -> Collection<G, (K, D), isize>;
}

pub trait Aggregate<'a, G: Scope, D: Data, K: Data + Hashable, T: Factor<'a, G, D, K>>
where
    G::Timestamp: Lattice + Ord,
{
    /// Implements the given aggregate for the generic datatype D
    fn implement(self, factor: T, var: u32) -> T;
}

pub trait InsideOut<'a, G: Scope, D: Data, K: Data + Hashable, T: Factor<'a, G, D, K>>
where
    G::Timestamp: Lattice + Ord,
{
    /// An application of insideOut for generic Factor T
    fn inside_out(self) -> T;
}

pub struct Query<T, A> {
    pub factors: Vec<T>,
    pub aggregates: Vec<A>,
    pub variable_order: Vec<u32>,
}

pub fn eliminate<
    'a,
    G: Scope,
    D: Data,
    K: Data + Hashable,
    T: Factor<'a, G, D, K>,
    A: Aggregate<'a, G, D, K, T>,
>(
    mut factors: Vec<T>,
    aggregate: A,
    var: u32,
) -> T
where
    G::Timestamp: Lattice + Ord,
{
    // Recursivly joins all factors
    let left = factors.remove(0);
    let joined = factors.into_iter().fold(left, |factor, next| {
        let mut vertices = factor.vertices().clone();
        let next_vertices = next.vertices().clone();
        let join_vars = vertices
            .drain_filter(|x| next.vertices().contains(x))
            .collect();

        let tuples = factor
            .tuples_by_variables(&join_vars)
            .join(&next.tuples_by_variables(&join_vars));

        T::normalize(
            vertex_union(
                &join_vars.into_iter().chain(vertices.into_iter()).collect(),
                &next_vertices,
            ),
            tuples,
        )
    });
    // Aggregate `var` with `aggregate`
    aggregate.implement(joined, var)
}
// TODO Run FAQ in iterative child scope
impl<
        'a,
        G: Scope,
        D: Data,
        K: Data + Hashable,
        T: Factor<'a, G, D, K>,
        A: Aggregate<'a, G, D, K, T> + Clone,
    > InsideOut<'a, G, D, K, T> for Query<T, A>
where
    G::Timestamp: Lattice + Ord,
{
    fn inside_out(self) -> T {
        let zipped: Vec<(u32, A)> = self
            .variable_order
            .iter()
            .cloned()
            .zip(self.aggregates.iter().cloned())
            .collect();
        // Reduce over factors, vertices and aggregates to return a single Factor
        let mut faq = zipped
            .into_iter()
            .fold(self.factors, |mut factors, (var, agg)| {
                let hyper_edges: Vec<T> = factors.drain_filter(|x| x.participate(&var)).collect();
                let factor_prime = eliminate(hyper_edges, agg, var);
                factors.push(factor_prime);
                factors
            });
        // TODO Implement faqs free variable join
        let output = faq.remove(0);
        output
    }
}
