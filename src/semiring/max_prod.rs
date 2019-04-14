//! Semirring implementations MaxProd

use differential_dataflow::difference::Monoid;
use std::ops::{Add, Mul};

use Convert;

#[derive(
    Abomonation, Copy, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash,
)]
pub struct MaxProd {
    pub value: u32,
}

impl Add<Self> for MaxProd {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        MaxProd {
            value: std::cmp::max(self.value, rhs.value),
        }
    }
}

impl Mul<Self> for MaxProd {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self {
        MaxProd {
            value: self.value * rhs.value,
        }
    }
}

impl Monoid for MaxProd {
    fn zero() -> MaxProd {
        MaxProd { value: 0 }
    }
}

impl Convert for MaxProd {
    fn from(value: isize) -> MaxProd {
        MaxProd {
            value: value.abs() as u32,
        }
    }
}
