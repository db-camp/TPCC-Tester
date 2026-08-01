//! Unified dispatch for one immutable ranked transaction.

use crate::connection::client::RmdbClient;
use crate::profile::TransactionKind;
use crate::transaction::TransactionType;
use crate::workload::{TransactionParameters, TransactionTicket};

use super::runner::{RankedTransactionError, RankedTransactionOutcome};

#[derive(Clone, Debug)]
pub struct FrozenTransaction {
    ticket: TransactionTicket,
    timestamp: String,
    fingerprint: u64,
}

impl FrozenTransaction {
    pub fn new(ticket: TransactionTicket, timestamp: String) -> Result<Self, &'static str> {
        if timestamp.is_empty() || timestamp.len() > 30 || timestamp.as_bytes().contains(&0) {
            return Err("ranked timestamp must be a nonempty, NUL-free CHAR(30) value");
        }
        let fingerprint = fingerprint_with_timestamp(ticket.parameter_fingerprint(), &timestamp);
        Ok(Self {
            ticket,
            timestamp,
            fingerprint,
        })
    }

    pub fn ticket(&self) -> &TransactionTicket {
        &self.ticket
    }

    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }

    pub const fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    pub fn transaction_type(&self) -> TransactionType {
        transaction_type(self.ticket.kind())
    }

    pub fn expects_business_rollback(&self) -> bool {
        matches!(
            self.ticket.parameters(),
            TransactionParameters::NewOrder(input) if input.expected_rollback()
        )
    }
}

pub async fn execute(
    client: &mut RmdbClient,
    frozen: &FrozenTransaction,
) -> Result<RankedTransactionOutcome, RankedTransactionError> {
    let route = frozen.ticket.route();
    let result = match frozen.ticket.parameters() {
        TransactionParameters::NewOrder(input) => {
            super::new_order::execute(client, route, input, &frozen.timestamp).await
        }
        TransactionParameters::Payment(input) => {
            super::payment::execute(client, route, input, &frozen.timestamp).await
        }
        TransactionParameters::OrderStatus(input) => {
            super::order_status::execute(client, route, input).await
        }
        TransactionParameters::Delivery(input) => {
            super::delivery::execute(client, route, input, &frozen.timestamp).await
        }
        TransactionParameters::StockLevel(input) => {
            super::stock_level::execute(client, route, input).await
        }
    };
    if let Err(RankedTransactionError::Semantic(semantic)) = &result {
        eprintln!(
            "[TDISPATCH] {:?} semantic failure: {}; route={:?}",
            frozen.ticket.kind(),
            semantic,
            route
        );
    }
    result
}

pub const fn transaction_type(kind: TransactionKind) -> TransactionType {
    match kind {
        TransactionKind::NewOrder => TransactionType::NewOrder,
        TransactionKind::Payment => TransactionType::Payment,
        TransactionKind::OrderStatus => TransactionType::OrderStatus,
        TransactionKind::Delivery => TransactionType::Delivery,
        TransactionKind::StockLevel => TransactionType::StockLevel,
    }
}

fn fingerprint_with_timestamp(seed: u64, timestamp: &str) -> u64 {
    let mut hash = seed ^ 0xcbf2_9ce4_8422_2325;
    for byte in timestamp.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use crate::routing::{ClientSequence, OfficialRouter, StageId, WorkloadSeed};
    use crate::workload::Final2026Workload;

    use super::*;

    #[test]
    fn frozen_retry_keeps_selection_timestamp_and_identity() {
        let router = OfficialRouter::new(WorkloadSeed(2026));
        let wheel = router.wheel(StageId::measurement(0));
        let workload = Final2026Workload::new(&router, &wheel);
        let mut sequence = ClientSequence::new(7).unwrap();
        let ticket = workload.select(&mut sequence).unwrap();
        let retry = ticket.retry();

        let left = FrozenTransaction::new(ticket, "2026-07-29 17:30:00".to_owned()).unwrap();
        let right = FrozenTransaction::new(retry, "2026-07-29 17:30:00".to_owned()).unwrap();

        assert!(left.ticket().shares_selection_with(right.ticket()));
        assert_eq!(left.timestamp(), right.timestamp());
        assert_eq!(left.fingerprint(), right.fingerprint());
    }

    #[test]
    fn timestamp_is_part_of_the_scheduler_identity() {
        let router = OfficialRouter::new(WorkloadSeed(9));
        let wheel = router.wheel(StageId::WARMUP);
        let workload = Final2026Workload::new(&router, &wheel);
        let mut sequence = ClientSequence::new(0).unwrap();
        let ticket = workload.select(&mut sequence).unwrap();

        let left =
            FrozenTransaction::new(ticket.clone(), "2026-07-29 17:30:00".to_owned()).unwrap();
        let right = FrozenTransaction::new(ticket, "2026-07-29 17:30:01".to_owned()).unwrap();
        assert_ne!(left.fingerprint(), right.fingerprint());
    }
}
