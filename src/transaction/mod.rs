pub mod new_order;
pub mod payment;
pub mod delivery;
pub mod order_status;
pub mod stock_level;

use rand::Rng;

use crate::connection::cursor::RmdbCursor;
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

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

pub fn select_transaction_type(
    rw_ratio: f64,
    txn_probs: &[f64],
) -> TransactionType {
    let mut rng = rand::rng();
    let types = TransactionType::all();

    if rng.random::<f64>() < rw_ratio {
        // Read-write: NewOrder, Payment, Delivery
        weighted_choice(&mut rng, &[txn_probs[0], txn_probs[1], txn_probs[2], 0.0, 0.0], types)
    } else {
        // Read-only: OrderStatus, StockLevel
        weighted_choice(&mut rng, &[0.0, 0.0, 0.0, txn_probs[3], txn_probs[4]], types)
    }
}

fn weighted_choice(
    rng: &mut impl Rng,
    weights: &[f64],
    items: &[TransactionType],
) -> TransactionType {
    let total: f64 = weights.iter().sum();
    if total <= 0.0 {
        return TransactionType::NewOrder;
    }
    let mut r = rng.random::<f64>() * total;
    for (i, &w) in weights.iter().enumerate() {
        r -= w;
        if r <= 0.0 {
            return items[i];
        }
    }
    items[0]
}
