//! Semirring implementations for Differentials Differences
//!
//! The Differences type require the implementation of the Monoid trait.

pub mod max_prod;
pub mod sum_prod;

pub trait Convert {
    fn from(value: isize) -> Self;
}

impl Convert for isize {
    fn from(value: isize) -> isize {
        value
    }
}

impl Convert for i64 {
    fn from(value: isize) -> i64 {
        value as i64
    }
}

impl Convert for i32 {
    fn from(value: isize) -> i32 {
        value as i32
    }
}
