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
use differential_dataflow::operators::Count;
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
    match variables.split_first() {
        Some((first, rest)) => rest.iter().fold(first.to_vec(), |acc, right| {
            acc.iter()
                .filter(|x| !right.contains(x))
                .chain(right.iter())
                .cloned()
                .collect()
        }),
        None => vec![],
    }
}

// Intersection of variables of multiple factors
pub fn intersection<T: PartialEq + Clone>(variables: &Vec<Vec<T>>) -> Vec<T> {
    match variables.split_first() {
        Some((first, rest)) => first
            .iter()
            .cloned()
            .filter(|var| rest.iter().all(|x| x.contains(&var)))
            .collect(),
        None => vec![],
    }
}

pub trait Factor<'a, G: Scope>
where
    G::Timestamp: Lattice + Ord,
{
    /// Create a new factor from variables and a Collection
    fn new(vertices: Vec<u32>, tuples: Collection<G, Vec<Value>, isize>) -> Self;
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

pub trait InsideOut<'a, G: Scope, T: Factor<'a, G>>
where
    G::Timestamp: Lattice + Ord,
{
    /// An application of insideOut for generic Factor T
    fn inside_out(self) -> T;
}

pub struct Query<T> {
    pub factors: Vec<T>,
    pub variable_order: Vec<u32>,
}

// Joining factors alongside their least common denominator
pub fn join<'a, G: Scope, T: Factor<'a, G>>(mut factors: Vec<T>) -> T
where
    G::Timestamp: Lattice + Ord,
{
    // Compute the intersection of all factors
    let variables: Vec<Vec<u32>> = factors.iter().map(|f| f.vertices()).collect();
    let join_vars = intersection(&variables);

    // Join all factors
    let left = factors.remove(0);
    let input: Vec<Collection<G, (Vec<Value>, Vec<Value>), isize>> = factors
        .into_iter()
        .map(|f| f.tuples_by_variables(&join_vars))
        .collect();
    let tuples = input
        .iter()
        .fold(left.tuples_by_variables(&join_vars), |left, right| {
            left.join_map(&right, |k, val1, val2| {
                (
                    k.clone(),
                    val1.iter().cloned().chain(val2.iter().cloned()).collect(),
                )
            })
        });

    // Normalize: (K, V) -> V, potentially enforcing factor specific invariants
    T::normalize(union(&variables), tuples)
}

// Eliminate the given variable
pub fn eliminate<'a, G: Scope, T: Factor<'a, G>>(factor: T, var: u32) -> T
where
    G::Timestamp: Lattice + Ord,
{
    T::new(
        factor
            .vertices()
            .iter()
            .filter(|x| **x != var)
            .cloned()
            .collect(),
        factor
            .tuples()
            .count()
            .explode(|(val, count)| Some((val, count))),
    )
}

impl<'a, G: Scope, T: Factor<'a, G>> InsideOut<'a, G, T> for Query<T>
where
    G::Timestamp: Lattice + Ord,
{
    fn inside_out(self) -> T {
        // Reduce over factors, vertices and aggregates to return an faq instance over only free variables
        let faq = self
            .variable_order
            .into_iter()
            .fold(self.factors, |mut factors, var| {
                let hyper_edges: Vec<T> = factors.drain_filter(|x| x.participate(&var)).collect();
                let factor_prime = eliminate(join(hyper_edges), var);
                factors.push(factor_prime);
                factors
            });
        // Join the remaining factors to produce the output representation
        let output = join(faq);
        output
    }
}
