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
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Join as JoinMap;

pub mod factors;

extern crate abomonation;
#[macro_use]
extern crate serde_derive;

/// Possible data values.
///
/// This enum captures the currently supported data types, and is the least common denominator
/// for the types of records moved around.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// A string
    String(String),
    /// A boolean
    Bool(bool),
    /// A 64 bit signed integer
    Number(i64),
}

// Union of variables of multiple factors
pub fn union<T: PartialEq + Clone>(variables: &Vec<Vec<T>>) -> Vec<T> {
    let (first, rest) = variables.split_first().unwrap();
    rest.iter().fold(first.to_vec(), |acc, right| {
        acc.iter()
            .filter(|x| !right.contains(x))
            .chain(right.iter())
            .cloned()
            .collect()
    })
}

// Intersection of variables of multiple factors
pub fn intersection<T: PartialEq + Clone>(variables: &Vec<Vec<T>>) -> Vec<T> {
    let (first, rest) = variables.split_first().unwrap();
    first
        .iter()
        .cloned()
        .filter(|var| rest.iter().all(|x| x.contains(&var)))
        .collect()
}

pub trait Factor<'a, G: Scope>
where
    G::Timestamp: Lattice + Ord,
{
    /// Creates a new factor from the joined collection
    fn normalize(
        vertices: Vec<u32>,
        tuples: Collection<G, (Vec<Value>, Vec<Value>), isize>,
    ) -> Self;
    /// List the vertices for a given factor
    fn vertices(&self) -> Vec<u32>;
    /// A collection containing all tuples
    fn tuples(self) -> Collection<G, Vec<Value>, isize>;
    /// Determine if the given factor should participate in insideOut
    fn participate(&self, var: &u32) -> bool;
    /// Arrange the tuples, s.t. we can join
    fn tuples_by_variables(self, vars: &Vec<u32>)
        -> Collection<G, (Vec<Value>, Vec<Value>), isize>;
}

pub trait Aggregate<'a, G: Scope,T: Factor<'a, G>>
where
    G::Timestamp: Lattice + Ord,
{
    /// Implements the given aggregate for the generic datatype D
    fn implement(self, factor: T, var: u32) -> T;
}

pub trait InsideOut<'a, G: Scope, T: Factor<'a, G>>
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

pub fn join_factors<'a, G: Scope,T: Factor<'a, G>>(
    mut factors: Vec<T>,
) -> T
where
    G::Timestamp: Lattice + Ord,
{
    // Compute the intersection of all factors
    let variables: Vec<Vec<u32>> = factors.iter().map(|f| f.vertices()).collect();
    let join_vars = intersection(&variables);

    // Recursivly joins all factors
    let left = factors.remove(0);
    factors.into_iter().fold(left, |factor, next| {
        let variables = union(&vec![factor.vertices(), next.vertices()]);

        let tuples = factor
            .tuples_by_variables(&join_vars)
            .join(&next.tuples_by_variables(&join_vars));

        T::normalize(variables, tuples)
    })
}

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
        // Reduce over factors, vertices and aggregates to return an faq instance over only free variables
        let faq = zipped
            .into_iter()
            .fold(self.factors, |mut factors, (var, agg)| {
                let hyper_edges: Vec<T> = factors.drain_filter(|x| x.participate(&var)).collect();
                let factor_prime = join_factors(hyper_edges);
                factors.push(agg.implement(factor_prime, var));
                factors
            });
        // Join the remaining factors to produce the output representation
        let output = join_factors(faq);
        output
    }
}
