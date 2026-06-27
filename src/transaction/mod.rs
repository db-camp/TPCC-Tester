pub mod delivery;
pub mod new_order;
pub mod order_status;
pub mod payment;
pub mod stock_level;

use crate::connection::cursor::RmdbCursor;
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

const SCHEDULE_SCALE: f64 = 1000.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionType {
    NewOrder = 0,
    Payment = 1,
    Delivery = 2,
    OrderStatus = 3,
    StockLevel = 4,
}

impl TransactionType {
    pub fn name(&self) -> &'static str {
        match self {
            TransactionType::NewOrder => "NewOrder",
            TransactionType::Payment => "Payment",
            TransactionType::Delivery => "Delivery",
            TransactionType::OrderStatus => "OrderStatus",
            TransactionType::StockLevel => "StockLevel",
        }
    }

    pub fn all() -> &'static [TransactionType] {
        &[
            TransactionType::NewOrder,
            TransactionType::Payment,
            TransactionType::Delivery,
            TransactionType::OrderStatus,
            TransactionType::StockLevel,
        ]
    }
}

pub async fn execute_transaction(
    cursor: &mut RmdbCursor,
    gen: &TpccDataGen,
    txn_type: TransactionType,
) -> Result<bool, TpccError> {
    match txn_type {
        TransactionType::NewOrder => new_order::execute(cursor, gen).await,
        TransactionType::Payment => payment::execute(cursor, gen).await,
        TransactionType::Delivery => delivery::execute(cursor, gen).await,
        TransactionType::OrderStatus => order_status::execute(cursor, gen).await,
        TransactionType::StockLevel => stock_level::execute(cursor, gen).await,
    }
}

#[derive(Debug)]
pub struct TransactionSchedule {
    cycle: Vec<TransactionType>,
}

impl TransactionSchedule {
    pub fn new(rw_ratio: f64, txn_probs: &[f64]) -> Self {
        let weights = effective_weights(rw_ratio, txn_probs);
        Self {
            cycle: smooth_weighted_cycle(&weights),
        }
    }

    pub fn pick(&self, sequence: u64) -> TransactionType {
        if self.cycle.is_empty() {
            return TransactionType::NewOrder;
        }
        self.cycle[sequence as usize % self.cycle.len()]
    }

    pub fn describe(&self) -> String {
        let mut counts = std::collections::HashMap::new();
        for txn_type in &self.cycle {
            *counts.entry(*txn_type).or_insert(0usize) += 1;
        }
        TransactionType::all()
            .iter()
            .map(|txn_type| {
                format!(
                    "{}={}",
                    txn_type.name(),
                    counts.get(txn_type).copied().unwrap_or(0)
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn effective_weights(rw_ratio: f64, txn_probs: &[f64]) -> Vec<(TransactionType, f64)> {
    let get = |idx: usize| txn_probs.get(idx).copied().unwrap_or(0.0).max(0.0);
    let new_order = get(0);
    let payment = get(1);
    let order_status = get(2);
    let delivery = get(3);
    let stock_level = get(4);

    let rw_ratio = rw_ratio.clamp(0.0, 1.0);
    let rw_total = new_order + payment + delivery;
    let ro_total = order_status + stock_level;
    let rw_scale = if rw_total > 0.0 {
        rw_ratio / rw_total
    } else {
        0.0
    };
    let ro_scale = if ro_total > 0.0 {
        (1.0 - rw_ratio) / ro_total
    } else {
        0.0
    };

    vec![
        (TransactionType::NewOrder, new_order * rw_scale),
        (TransactionType::Payment, payment * rw_scale),
        (TransactionType::Delivery, delivery * rw_scale),
        (TransactionType::OrderStatus, order_status * ro_scale),
        (TransactionType::StockLevel, stock_level * ro_scale),
    ]
}

fn smooth_weighted_cycle(weights: &[(TransactionType, f64)]) -> Vec<TransactionType> {
    let mut integer_weights: Vec<(TransactionType, i64)> = weights
        .iter()
        .map(|(txn_type, weight)| (*txn_type, (weight * SCHEDULE_SCALE).round() as i64))
        .filter(|(_, weight)| *weight > 0)
        .collect();

    if integer_weights.is_empty() {
        integer_weights.push((TransactionType::NewOrder, 1));
    }

    let total: i64 = integer_weights.iter().map(|(_, weight)| *weight).sum();
    let mut current = vec![0i64; integer_weights.len()];
    let mut cycle = Vec::with_capacity(total as usize);

    for _ in 0..total {
        let mut best = 0usize;
        for (idx, (_, weight)) in integer_weights.iter().enumerate() {
            current[idx] += *weight;
            if current[idx] > current[best] {
                best = idx;
            }
        }
        current[best] -= total;
        cycle.push(integer_weights[best].0);
    }

    cycle
}
