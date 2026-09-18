//! Strategy components: inventory, quoting, side protection, active reduction
//! and risk gates. All modules are pure state machines / functions so they can
//! be unit-tested without any exchange connectivity.

pub mod inventory;
pub mod protection;
pub mod quote;
pub mod reduce;
pub mod risk;
