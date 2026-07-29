//! Public final-2026 ranked workload building blocks.

pub mod bounded_stats;
pub mod catalog;
pub mod common;
pub(crate) mod core_artifact_codec;
pub mod delivery;
pub mod dispatch;
pub mod evidence_collector;
pub mod ledger;
pub mod new_order;
pub mod order_status;
pub mod payment;
pub mod payment_endpoints;
pub mod preflight;
pub mod recovery_samples;
pub mod rich_recovery_samples;
pub mod runner;
pub mod session;
pub mod stock_level;
pub mod terminal_evidence;
