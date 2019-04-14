//! Semirring implementations SumProd

use differential_dataflow::difference::Monoid;
use std::cmp::Ordering;
use std::ops::{AddAssign, Neg, Mul};

use Convert;

fn f32_bits(a: f32) -> i32 {
    unsafe { std::mem::transmute(a) }
}

#[derive(Abomonation, Copy, PartialOrd, Debug, Clone, Serialize, Deserialize)]
pub struct SumProd {
    pub value: f32,
}

impl PartialEq for SumProd {
    fn eq(&self, other: &SumProd) -> bool {
        let mut a = f32_bits(self.value);
        let mut b = f32_bits(other.value);
        if a < 0 {
            a ^= 0x7fffffff;
        }
        if b < 0 {
            b ^= 0x7fffffff;
        }
        a == b
    }
}

impl Eq for SumProd {}

impl Ord for SumProd {
    fn cmp(&self, other: &SumProd) -> Ordering {
        let mut a = f32_bits(self.value);
        let mut b = f32_bits(other.value);
        if a < 0 {
            a ^= 0x7fffffff;
        }
        if b < 0 {
            b ^= 0x7fffffff;
        }
        a.cmp(&b)
    }
}

impl<'a> AddAssign<&'a Self> for SumProd {
    fn add_assign(&mut self, rhs: &SumProd) {
        *self = SumProd {
            value: self.value + rhs.value,
        }
    }
}

// impl AddAssign<Self> for SumProd {
//     type Output = Self;
//     fn add(self, rhs: Self) -> Self {
//         SumProd {
//             value: self.value + rhs.value,
//         }
//     }
// }

impl Mul<Self> for SumProd {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self {
        SumProd {
            value: self.value * rhs.value,
        }
    }
}

impl Monoid for SumProd {
    fn zero() -> SumProd {
        SumProd { value: 0.0 }
    }
}

impl Convert for SumProd {
    fn from(value: isize) -> SumProd {
        SumProd {
            value: value as f32,
        }
    }
}
