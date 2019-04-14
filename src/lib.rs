//! Core infrastructure
//!
//! This crate contains types and logic for Functional Aggregate Queries (FAQ).
//! Every factor in the FAQ query consists of variables and
//! resembles one hyperedge in the queries hypergraph.
//! We represent every factor as one differential collection.
//! Computing the FAQ query is an iterative elimination of all bound
//! variables.

#![feature(drain_filter)]
extern crate differential_dataflow;
extern crate timely;

use std::ops::Mul;

use timely::dataflow::operators::*;
use timely::dataflow::*;

use differential_dataflow::collection::{AsCollection, Collection};
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Join as JoinMap;
use differential_dataflow::operators::{Consolidate, Count};

use semiring::Convert;

pub mod factors;
pub mod semiring;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;
#[macro_use]
extern crate serde_derive;
extern crate serde;

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

pub trait Factor<'a, G: Scope, D>: ::std::marker::Sized
where
    G::Timestamp: Lattice + Ord,
    D: Monoid + Mul<Output = D> + Convert,
{
    /// Create a new factor from variables and a Collection
    fn new(variables: Vec<u32>, tuples: Collection<G, Vec<Value>, D>) -> Self;
    /// Creates a new factor from the joined collection
    fn normalize(
        tuples: Collection<G, (Vec<Value>, Vec<Value>), D>,
        pos: usize,
    ) -> Collection<G, Vec<Value>, D> {
        tuples.map(move |(k, v)| {
            let mut rem: Vec<Value> = k.iter().enumerate().filter(|(i, _x)| *i != pos).map(|(_i, x)| x.clone()).collect();
            let mut values: Vec<Value> = v.clone();
            values.append(&mut rem);
            values
        })
            // .inspect(|x|println!("{:?}", x))
    }
    /// List the variables for a given factor
    fn variables(&self) -> Vec<u32>;
    /// A collection containing all tuples
    fn tuples(self) -> Collection<G, Vec<Value>, D>;
    /// Determine if the given factor should participate in insideOut
    fn participate(&self, var: &u32) -> bool {
        self.variables().contains(&var)
    }
    /// Arrange the tuples, s.t. we can join
    fn tuples_by_variables(self, vars: &Vec<u32>) -> Collection<G, (Vec<Value>, Vec<Value>), D> {
        let pos: Vec<usize> = vars
            .into_iter()
            .flat_map(|x| self.variables().iter().position(|&v| x == &v))
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

pub trait InsideOut<'a, G: Scope, D, T: Factor<'a, G, D>>
where
    G::Timestamp: Lattice + Ord,
    D: Monoid + Mul<Output = D> + Convert,
{
    /// An application of insideOut for generic Factor T
    fn inside_out(self) -> T;
}

pub struct Query<T> {
    pub factors: Vec<T>,
    pub variable_order: Vec<u32>,
}

// Joining factors alongside their least common denominator
pub fn join<'a, G: Scope, D, T: Factor<'a, G, D>>(
    mut factors: Vec<T>,
) -> (
    Vec<u32>,
    Vec<u32>,
    Collection<G, (Vec<Value>, Vec<Value>), D>,
)
where
    G::Timestamp: Lattice + Ord,
    D: Monoid + Mul<Output = D> + Convert,
{
    if factors.len() == 1 {
        let factor = factors.remove(0);
        let variables = factor.variables();
        (
            variables.clone(),
            variables.clone(),
            factor.tuples_by_variables(&variables),
        )
    } else {
        // Compute the intersection of all factors
        let variables: Vec<Vec<u32>> = factors.iter().map(|f| f.variables()).collect();
        let join_vars = intersection(&variables);

        // Join all factors
        let left = factors.remove(0);
        let input: Vec<Collection<G, (Vec<Value>, Vec<Value>), D>> = factors
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

        (union(&variables), join_vars, tuples)
    }
}

// Eliminate the given variable
pub fn eliminate<'a, G: Scope, D, T: Factor<'a, G, D>>(
    joined: (
        Vec<u32>,
        Vec<u32>,
        Collection<G, (Vec<Value>, Vec<Value>), D>,
    ),
    var: u32,
) -> T
where
    G::Timestamp: Lattice + Ord,
    D: Monoid + Mul<Output = D> + Convert,
{
    let (variables, join_vars, tuples) = joined;
    let pos:usize = join_vars.iter().position(|&v| var == v).unwrap();

    T::new(
        variables.iter().filter(|x| **x != var).cloned().collect(),
        T::normalize(tuples, pos).consolidate(),
    )
}

impl<'a, G: Scope, D, T: Factor<'a, G, D>> InsideOut<'a, G, D, T> for Query<T>
where
    G::Timestamp: Lattice + Ord,
    D: Monoid + Mul<Output = D> + Convert,
{
    fn inside_out(self) -> T {
        // Reduce over factors, variables and aggregates to return an faq instance over only free variables
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
        let (vars, _join_vars, tuples) = join(faq);
        T::new(
            vars,
            tuples.map(|(k, v)| k.iter().chain(v.iter()).cloned().collect()),
        )
    }
}
