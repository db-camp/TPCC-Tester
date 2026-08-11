//! Prepared statement catalogue for the public final-2026 ranked workload.
//!
//! The catalogue describes relational operations, not a hidden judge SQL
//! template.  A later planner expands the stage templates using one immutable
//! transaction input and sends each expanded stage as one `EXEC_BATCH`.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use crate::connection::prepared::{Statement, StatementKind};
use crate::connection::wire::{Column, SqlType};
use crate::runtime_schema::{RuntimeSchema, StatementLayout};

pub const UNDELIVERED_CARRIER_ID: i32 = 0;
pub const UNDELIVERED_DATE: &str = "";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u16)]
pub enum StatementId {
    Begin = 1,
    Commit = 2,
    Abort = 3,

    NewOrderHome = 10,
    NewOrderLockStock = 11,
    NewOrderItem = 12,
    NewOrderStock = 13,
    NewOrderAdvanceDistrict = 14,
    NewOrderInsertOrder = 15,
    NewOrderInsertQueue = 16,
    NewOrderUpdateStockNormal = 17,
    NewOrderUpdateStockWrapped = 18,
    NewOrderInsertLine = 19,

    PaymentWarehouse = 30,
    PaymentUpdateWarehouse = 31,
    PaymentDistrict = 32,
    PaymentUpdateDistrict = 33,
    PaymentCustomerById = 34,
    PaymentCustomerByLast = 35,
    PaymentUpdateGoodCustomer = 36,
    PaymentUpdateBadCustomer = 37,
    PaymentInsertHistory = 38,
    PaymentCustomerAfter = 39,

    OrderStatusCustomerById = 50,
    OrderStatusCustomerByLast = 51,
    OrderStatusLatestOrder = 52,
    OrderStatusOrder = 53,
    OrderStatusLines = 54,

    DeliveryOldestOrder = 70,
    DeliveryLockQueue = 71,
    DeliveryConfirmQueue = 72,
    DeliveryOrder = 73,
    DeliveryCustomer = 74,
    DeliveryLineRows = 75,
    DeliveryLineSum = 76,
    DeliveryDeleteQueue = 77,
    DeliveryUpdateOrder = 78,
    DeliveryUpdateLines = 79,
    DeliveryUpdateCustomer = 80,
    DeliveryCustomerAfter = 81,

    StockLevelNextOrder = 89,
    StockLevelCount = 90,
}

impl StatementId {
    pub const ALL: [Self; 42] = [
        Self::Begin,
        Self::Commit,
        Self::Abort,
        Self::NewOrderHome,
        Self::NewOrderLockStock,
        Self::NewOrderItem,
        Self::NewOrderStock,
        Self::NewOrderAdvanceDistrict,
        Self::NewOrderInsertOrder,
        Self::NewOrderInsertQueue,
        Self::NewOrderUpdateStockNormal,
        Self::NewOrderUpdateStockWrapped,
        Self::NewOrderInsertLine,
        Self::PaymentWarehouse,
        Self::PaymentUpdateWarehouse,
        Self::PaymentDistrict,
        Self::PaymentUpdateDistrict,
        Self::PaymentCustomerById,
        Self::PaymentCustomerByLast,
        Self::PaymentUpdateGoodCustomer,
        Self::PaymentUpdateBadCustomer,
        Self::PaymentInsertHistory,
        Self::PaymentCustomerAfter,
        Self::OrderStatusCustomerById,
        Self::OrderStatusCustomerByLast,
        Self::OrderStatusLatestOrder,
        Self::OrderStatusOrder,
        Self::OrderStatusLines,
        Self::DeliveryOldestOrder,
        Self::DeliveryLockQueue,
        Self::DeliveryConfirmQueue,
        Self::DeliveryOrder,
        Self::DeliveryCustomer,
        Self::DeliveryLineRows,
        Self::DeliveryLineSum,
        Self::DeliveryDeleteQueue,
        Self::DeliveryUpdateOrder,
        Self::DeliveryUpdateLines,
        Self::DeliveryUpdateCustomer,
        Self::DeliveryCustomerAfter,
        Self::StockLevelNextOrder,
        Self::StockLevelCount,
    ];

    pub const fn wire_id(self) -> u16 {
        self as u16
    }

    pub const fn key(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::Commit => "commit",
            Self::Abort => "abort",
            Self::NewOrderHome => "new_order.home",
            Self::NewOrderLockStock => "new_order.lock_stock",
            Self::NewOrderItem => "new_order.item",
            Self::NewOrderStock => "new_order.stock",
            Self::NewOrderAdvanceDistrict => "new_order.advance_district",
            Self::NewOrderInsertOrder => "new_order.insert_order",
            Self::NewOrderInsertQueue => "new_order.insert_queue",
            Self::NewOrderUpdateStockNormal => "new_order.update_stock_normal",
            Self::NewOrderUpdateStockWrapped => "new_order.update_stock_wrapped",
            Self::NewOrderInsertLine => "new_order.insert_line",
            Self::PaymentWarehouse => "payment.warehouse",
            Self::PaymentUpdateWarehouse => "payment.update_warehouse",
            Self::PaymentDistrict => "payment.district",
            Self::PaymentUpdateDistrict => "payment.update_district",
            Self::PaymentCustomerById => "payment.customer_by_id",
            Self::PaymentCustomerByLast => "payment.customer_by_last",
            Self::PaymentUpdateGoodCustomer => "payment.update_good_customer",
            Self::PaymentUpdateBadCustomer => "payment.update_bad_customer",
            Self::PaymentInsertHistory => "payment.insert_history",
            Self::PaymentCustomerAfter => "payment.customer_after",
            Self::OrderStatusCustomerById => "order_status.customer_by_id",
            Self::OrderStatusCustomerByLast => "order_status.customer_by_last",
            Self::OrderStatusLatestOrder => "order_status.latest_order",
            Self::OrderStatusOrder => "order_status.order",
            Self::OrderStatusLines => "order_status.lines",
            Self::DeliveryOldestOrder => "delivery.oldest_order",
            Self::DeliveryLockQueue => "delivery.lock_queue",
            Self::DeliveryConfirmQueue => "delivery.confirm_queue",
            Self::DeliveryOrder => "delivery.order",
            Self::DeliveryCustomer => "delivery.customer",
            Self::DeliveryLineRows => "delivery.line_rows",
            Self::DeliveryLineSum => "delivery.line_sum",
            Self::DeliveryDeleteQueue => "delivery.delete_queue",
            Self::DeliveryUpdateOrder => "delivery.update_order",
            Self::DeliveryUpdateLines => "delivery.update_lines",
            Self::DeliveryUpdateCustomer => "delivery.update_customer",
            Self::DeliveryCustomerAfter => "delivery.customer_after",
            Self::StockLevelNextOrder => "stock_level.next_order",
            Self::StockLevelCount => "stock_level.count",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Multiplicity {
    Once,
    PerOrderLine,
    SortedUniqueStock,
    TenDistricts,
    PerClaimedDistrict,
}

/// One planner step.  A one-element `alternatives` slice is mandatory; a
/// longer slice means the planner chooses exactly one statement per expansion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanStep {
    pub alternatives: &'static [StatementId],
    pub multiplicity: Multiplicity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StageTemplate {
    pub round_trip: u8,
    pub steps: &'static [PlanStep],
}

const BEGIN: &[StatementId] = &[StatementId::Begin];
const COMMIT: &[StatementId] = &[StatementId::Commit];
const ABORT: &[StatementId] = &[StatementId::Abort];
const NEW_ORDER_HOME: &[StatementId] = &[StatementId::NewOrderHome];
const NEW_ORDER_DISTRICT: &[StatementId] = &[StatementId::StockLevelNextOrder];
const NEW_ORDER_LOCK_STOCK: &[StatementId] = &[StatementId::NewOrderLockStock];
const NEW_ORDER_ITEM: &[StatementId] = &[StatementId::NewOrderItem];
const NEW_ORDER_STOCK: &[StatementId] = &[StatementId::NewOrderStock];
const NEW_ORDER_ADVANCE_DISTRICT: &[StatementId] = &[StatementId::NewOrderAdvanceDistrict];
const NEW_ORDER_INSERT_ORDER: &[StatementId] = &[StatementId::NewOrderInsertOrder];
const NEW_ORDER_INSERT_QUEUE: &[StatementId] = &[StatementId::NewOrderInsertQueue];
const NEW_ORDER_UPDATE_STOCK: &[StatementId] = &[
    StatementId::NewOrderUpdateStockNormal,
    StatementId::NewOrderUpdateStockWrapped,
];
const NEW_ORDER_INSERT_LINE: &[StatementId] = &[StatementId::NewOrderInsertLine];

const PAYMENT_WAREHOUSE: &[StatementId] = &[StatementId::PaymentWarehouse];
const PAYMENT_UPDATE_WAREHOUSE: &[StatementId] = &[StatementId::PaymentUpdateWarehouse];
const PAYMENT_DISTRICT: &[StatementId] = &[StatementId::PaymentDistrict];
const PAYMENT_UPDATE_DISTRICT: &[StatementId] = &[StatementId::PaymentUpdateDistrict];
const PAYMENT_CUSTOMER: &[StatementId] = &[
    StatementId::PaymentCustomerById,
    StatementId::PaymentCustomerByLast,
];
const PAYMENT_UPDATE_CUSTOMER: &[StatementId] = &[
    StatementId::PaymentUpdateGoodCustomer,
    StatementId::PaymentUpdateBadCustomer,
];
const PAYMENT_INSERT_HISTORY: &[StatementId] = &[StatementId::PaymentInsertHistory];
const PAYMENT_CUSTOMER_AFTER: &[StatementId] = &[StatementId::PaymentCustomerAfter];

const ORDER_STATUS_CUSTOMER: &[StatementId] = &[
    StatementId::OrderStatusCustomerById,
    StatementId::OrderStatusCustomerByLast,
];
const ORDER_STATUS_LATEST_ORDER: &[StatementId] = &[StatementId::OrderStatusLatestOrder];
const ORDER_STATUS_ORDER: &[StatementId] = &[StatementId::OrderStatusOrder];
const ORDER_STATUS_LINES: &[StatementId] = &[StatementId::OrderStatusLines];

const DELIVERY_OLDEST_ORDER: &[StatementId] = &[StatementId::DeliveryOldestOrder];
const DELIVERY_LOCK_QUEUE: &[StatementId] = &[StatementId::DeliveryLockQueue];
const DELIVERY_CONFIRM_QUEUE: &[StatementId] = &[StatementId::DeliveryConfirmQueue];
const DELIVERY_ORDER: &[StatementId] = &[StatementId::DeliveryOrder];
const DELIVERY_CUSTOMER: &[StatementId] = &[StatementId::DeliveryCustomer];
const DELIVERY_LINE_ROWS: &[StatementId] = &[StatementId::DeliveryLineRows];
const DELIVERY_LINE_SUM: &[StatementId] = &[StatementId::DeliveryLineSum];
const DELIVERY_DELETE_QUEUE: &[StatementId] = &[StatementId::DeliveryDeleteQueue];
const DELIVERY_UPDATE_ORDER: &[StatementId] = &[StatementId::DeliveryUpdateOrder];
const DELIVERY_UPDATE_LINES: &[StatementId] = &[StatementId::DeliveryUpdateLines];
const DELIVERY_UPDATE_CUSTOMER: &[StatementId] = &[StatementId::DeliveryUpdateCustomer];
const DELIVERY_CUSTOMER_AFTER: &[StatementId] = &[StatementId::DeliveryCustomerAfter];

const STOCK_LEVEL_NEXT_ORDER: &[StatementId] = &[StatementId::StockLevelNextOrder];
const STOCK_LEVEL_COUNT: &[StatementId] = &[StatementId::StockLevelCount];

const NEW_ORDER_STAGE_ONE_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: BEGIN,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: NEW_ORDER_HOME,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: NEW_ORDER_DISTRICT,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: NEW_ORDER_LOCK_STOCK,
        multiplicity: Multiplicity::SortedUniqueStock,
    },
    PlanStep {
        alternatives: NEW_ORDER_ITEM,
        multiplicity: Multiplicity::PerOrderLine,
    },
    PlanStep {
        alternatives: NEW_ORDER_STOCK,
        multiplicity: Multiplicity::PerOrderLine,
    },
];

const NEW_ORDER_STAGE_TWO_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: NEW_ORDER_ADVANCE_DISTRICT,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: NEW_ORDER_INSERT_ORDER,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: NEW_ORDER_INSERT_QUEUE,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: NEW_ORDER_UPDATE_STOCK,
        multiplicity: Multiplicity::PerOrderLine,
    },
    PlanStep {
        alternatives: NEW_ORDER_INSERT_LINE,
        multiplicity: Multiplicity::PerOrderLine,
    },
    PlanStep {
        alternatives: COMMIT,
        multiplicity: Multiplicity::Once,
    },
];

const NEW_ORDER_ABORT_STEPS: &[PlanStep] = &[PlanStep {
    alternatives: ABORT,
    multiplicity: Multiplicity::Once,
}];

pub const NEW_ORDER_STAGES: &[StageTemplate] = &[
    StageTemplate {
        round_trip: 1,
        steps: NEW_ORDER_STAGE_ONE_STEPS,
    },
    StageTemplate {
        round_trip: 2,
        steps: NEW_ORDER_STAGE_TWO_STEPS,
    },
];

pub const NEW_ORDER_EXPECTED_ROLLBACK_STAGE: StageTemplate = StageTemplate {
    round_trip: 2,
    steps: NEW_ORDER_ABORT_STEPS,
};

const PAYMENT_STAGE_ONE_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: BEGIN,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: PAYMENT_WAREHOUSE,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: PAYMENT_UPDATE_WAREHOUSE,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: PAYMENT_WAREHOUSE,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: PAYMENT_DISTRICT,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: PAYMENT_UPDATE_DISTRICT,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: PAYMENT_DISTRICT,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: PAYMENT_CUSTOMER,
        multiplicity: Multiplicity::Once,
    },
];

const PAYMENT_STAGE_TWO_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: PAYMENT_UPDATE_CUSTOMER,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: PAYMENT_INSERT_HISTORY,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: PAYMENT_CUSTOMER_AFTER,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: COMMIT,
        multiplicity: Multiplicity::Once,
    },
];

pub const PAYMENT_STAGES: &[StageTemplate] = &[
    StageTemplate {
        round_trip: 1,
        steps: PAYMENT_STAGE_ONE_STEPS,
    },
    StageTemplate {
        round_trip: 2,
        steps: PAYMENT_STAGE_TWO_STEPS,
    },
];

const ORDER_STATUS_STAGE_ONE_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: BEGIN,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: ORDER_STATUS_CUSTOMER,
        multiplicity: Multiplicity::Once,
    },
];

const ORDER_STATUS_STAGE_TWO_STEPS: &[PlanStep] = &[PlanStep {
    alternatives: ORDER_STATUS_LATEST_ORDER,
    multiplicity: Multiplicity::Once,
}];

const ORDER_STATUS_STAGE_THREE_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: ORDER_STATUS_ORDER,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: ORDER_STATUS_LINES,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: COMMIT,
        multiplicity: Multiplicity::Once,
    },
];

pub const ORDER_STATUS_STAGES: &[StageTemplate] = &[
    StageTemplate {
        round_trip: 1,
        steps: ORDER_STATUS_STAGE_ONE_STEPS,
    },
    StageTemplate {
        round_trip: 2,
        steps: ORDER_STATUS_STAGE_TWO_STEPS,
    },
    StageTemplate {
        round_trip: 3,
        steps: ORDER_STATUS_STAGE_THREE_STEPS,
    },
];

const DELIVERY_STAGE_ONE_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: BEGIN,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: DELIVERY_OLDEST_ORDER,
        multiplicity: Multiplicity::TenDistricts,
    },
];

const DELIVERY_STAGE_TWO_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: DELIVERY_LOCK_QUEUE,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: DELIVERY_CONFIRM_QUEUE,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: DELIVERY_ORDER,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: DELIVERY_CUSTOMER,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: DELIVERY_LINE_ROWS,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: DELIVERY_LINE_SUM,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
];

const DELIVERY_STAGE_THREE_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: DELIVERY_DELETE_QUEUE,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: DELIVERY_UPDATE_ORDER,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: DELIVERY_UPDATE_LINES,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: DELIVERY_UPDATE_CUSTOMER,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: DELIVERY_CUSTOMER_AFTER,
        multiplicity: Multiplicity::PerClaimedDistrict,
    },
    PlanStep {
        alternatives: COMMIT,
        multiplicity: Multiplicity::Once,
    },
];

const DELIVERY_EMPTY_STAGE_TWO_STEPS: &[PlanStep] = &[PlanStep {
    alternatives: COMMIT,
    multiplicity: Multiplicity::Once,
}];

pub const DELIVERY_STAGES: &[StageTemplate] = &[
    StageTemplate {
        round_trip: 1,
        steps: DELIVERY_STAGE_ONE_STEPS,
    },
    StageTemplate {
        round_trip: 2,
        steps: DELIVERY_STAGE_TWO_STEPS,
    },
    StageTemplate {
        round_trip: 3,
        steps: DELIVERY_STAGE_THREE_STEPS,
    },
];

pub const DELIVERY_EMPTY_STAGE: StageTemplate = StageTemplate {
    round_trip: 2,
    steps: DELIVERY_EMPTY_STAGE_TWO_STEPS,
};

const STOCK_LEVEL_STAGE_ONE_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: BEGIN,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: STOCK_LEVEL_NEXT_ORDER,
        multiplicity: Multiplicity::Once,
    },
];

const STOCK_LEVEL_STAGE_TWO_STEPS: &[PlanStep] = &[
    PlanStep {
        alternatives: STOCK_LEVEL_COUNT,
        multiplicity: Multiplicity::Once,
    },
    PlanStep {
        alternatives: COMMIT,
        multiplicity: Multiplicity::Once,
    },
];

pub const STOCK_LEVEL_STAGES: &[StageTemplate] = &[
    StageTemplate {
        round_trip: 1,
        steps: STOCK_LEVEL_STAGE_ONE_STEPS,
    },
    StageTemplate {
        round_trip: 2,
        steps: STOCK_LEVEL_STAGE_TWO_STEPS,
    },
];

pub fn final2026_catalog() -> Vec<Statement> {
    use SqlType::{Char, Float32, Int32};

    vec![
        command(StatementId::Begin, &[], "BEGIN;"),
        command(StatementId::Commit, &[], "COMMIT;"),
        command(StatementId::Abort, &[], "ABORT;"),
        query(
            StatementId::NewOrderHome,
            &[Int32, Int32, Int32],
            "SELECT customer.c_discount AS c_discount, customer.c_last AS c_last, \
             customer.c_credit AS c_credit, warehouse.w_tax AS w_tax \
             FROM customer, warehouse \
             WHERE warehouse.w_id = $1 \
             AND customer.c_w_id = warehouse.w_id \
             AND customer.c_d_id = $2 AND customer.c_id = $3;",
            &[
                ("c_discount", Float32),
                ("c_last", Char),
                ("c_credit", Char),
                ("w_tax", Float32),
            ],
        ),
        command(
            StatementId::NewOrderLockStock,
            &[Int32, Int32],
            "UPDATE stock SET s_quantity = s_quantity \
             WHERE s_w_id = $1 AND s_i_id = $2;",
        ),
        query(
            StatementId::NewOrderItem,
            &[Int32],
            "SELECT i_id AS i_id, i_price AS i_price, i_name AS i_name, i_data AS i_data \
             FROM item WHERE i_id = $1;",
            &[
                ("i_id", Int32),
                ("i_price", Float32),
                ("i_name", Char),
                ("i_data", Char),
            ],
        ),
        query(
            StatementId::NewOrderStock,
            &[Int32, Int32],
            "SELECT s_quantity AS s_quantity, s_ytd AS s_ytd, \
             s_order_cnt AS s_order_cnt, s_remote_cnt AS s_remote_cnt, \
             s_data AS s_data, \
             s_dist_01 AS s_dist_01, s_dist_02 AS s_dist_02, \
             s_dist_03 AS s_dist_03, s_dist_04 AS s_dist_04, \
             s_dist_05 AS s_dist_05, s_dist_06 AS s_dist_06, \
             s_dist_07 AS s_dist_07, s_dist_08 AS s_dist_08, \
             s_dist_09 AS s_dist_09, s_dist_10 AS s_dist_10 \
             FROM stock WHERE s_w_id = $1 AND s_i_id = $2;",
            &[
                ("s_quantity", Int32),
                ("s_ytd", Float32),
                ("s_order_cnt", Int32),
                ("s_remote_cnt", Int32),
                ("s_data", Char),
                ("s_dist_01", Char),
                ("s_dist_02", Char),
                ("s_dist_03", Char),
                ("s_dist_04", Char),
                ("s_dist_05", Char),
                ("s_dist_06", Char),
                ("s_dist_07", Char),
                ("s_dist_08", Char),
                ("s_dist_09", Char),
                ("s_dist_10", Char),
            ],
        ),
        command(
            StatementId::NewOrderAdvanceDistrict,
            &[Int32, Int32],
            "UPDATE district SET d_next_o_id = d_next_o_id + 1 \
             WHERE d_w_id = $1 AND d_id = $2;",
        ),
        command(
            StatementId::NewOrderInsertOrder,
            &[Int32, Int32, Int32, Int32, Char, Int32, Int32, Int32],
            "INSERT INTO orders VALUES ($1, $2, $3, $4, $5, $6, $7, $8);",
        ),
        command(
            StatementId::NewOrderInsertQueue,
            &[Int32, Int32, Int32],
            "INSERT INTO new_orders VALUES ($1, $2, $3);",
        ),
        command(
            StatementId::NewOrderUpdateStockNormal,
            &[Int32, Float32, Int32, Int32, Int32, Int32],
            "UPDATE stock \
             SET s_quantity = s_quantity - $1, s_ytd = s_ytd + $2, \
             s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + $3 \
             WHERE s_w_id = $4 AND s_i_id = $5 AND s_quantity >= $6;",
        ),
        command(
            StatementId::NewOrderUpdateStockWrapped,
            &[Int32, Float32, Int32, Int32, Int32, Int32],
            "UPDATE stock \
             SET s_quantity = s_quantity - $1 + 91, s_ytd = s_ytd + $2, \
             s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + $3 \
             WHERE s_w_id = $4 AND s_i_id = $5 AND s_quantity < $6;",
        ),
        command(
            StatementId::NewOrderInsertLine,
            &[
                Int32, Int32, Int32, Int32, Int32, Int32, Char, Int32, Float32, Char,
            ],
            "INSERT INTO order_line \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);",
        ),
        query(
            StatementId::PaymentWarehouse,
            &[Int32],
            "SELECT w_ytd AS w_ytd, w_name AS w_name, w_street_1 AS w_street_1, \
             w_street_2 AS w_street_2, w_city AS w_city, w_state AS w_state, \
             w_zip AS w_zip FROM warehouse WHERE w_id = $1;",
            &[
                ("w_ytd", Float32),
                ("w_name", Char),
                ("w_street_1", Char),
                ("w_street_2", Char),
                ("w_city", Char),
                ("w_state", Char),
                ("w_zip", Char),
            ],
        ),
        command(
            StatementId::PaymentUpdateWarehouse,
            &[Float32, Int32],
            "UPDATE warehouse SET w_ytd = w_ytd + $1 WHERE w_id = $2;",
        ),
        query(
            StatementId::PaymentDistrict,
            &[Int32, Int32],
            "SELECT d_ytd AS d_ytd, d_name AS d_name, d_street_1 AS d_street_1, \
             d_street_2 AS d_street_2, d_city AS d_city, d_state AS d_state, \
             d_zip AS d_zip FROM district WHERE d_w_id = $1 AND d_id = $2;",
            &[
                ("d_ytd", Float32),
                ("d_name", Char),
                ("d_street_1", Char),
                ("d_street_2", Char),
                ("d_city", Char),
                ("d_state", Char),
                ("d_zip", Char),
            ],
        ),
        command(
            StatementId::PaymentUpdateDistrict,
            &[Float32, Int32, Int32],
            "UPDATE district SET d_ytd = d_ytd + $1 \
             WHERE d_w_id = $2 AND d_id = $3;",
        ),
        payment_customer_statement(StatementId::PaymentCustomerById, false),
        payment_customer_statement(StatementId::PaymentCustomerByLast, true),
        command(
            StatementId::PaymentUpdateGoodCustomer,
            &[Float32, Int32, Int32, Int32],
            "UPDATE customer \
             SET c_balance = c_balance - $1, \
             c_ytd_payment = c_ytd_payment + $1, \
             c_payment_cnt = c_payment_cnt + 1 \
             WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4;",
        ),
        command(
            StatementId::PaymentUpdateBadCustomer,
            &[Float32, Char, Int32, Int32, Int32],
            "UPDATE customer \
             SET c_balance = c_balance - $1, \
             c_ytd_payment = c_ytd_payment + $1, \
             c_payment_cnt = c_payment_cnt + 1, c_data = $2 \
             WHERE c_w_id = $3 AND c_d_id = $4 AND c_id = $5;",
        ),
        command(
            StatementId::PaymentInsertHistory,
            &[Int32, Int32, Int32, Int32, Int32, Char, Float32, Char],
            "INSERT INTO history VALUES ($1, $2, $3, $4, $5, $6, $7, $8);",
        ),
        query(
            StatementId::PaymentCustomerAfter,
            &[Int32, Int32, Int32],
            "SELECT c_balance AS c_balance, c_ytd_payment AS c_ytd_payment, \
             c_payment_cnt AS c_payment_cnt, c_delivery_cnt AS c_delivery_cnt, \
             c_data AS c_data \
             FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3;",
            &[
                ("c_balance", Float32),
                ("c_ytd_payment", Float32),
                ("c_payment_cnt", Int32),
                ("c_delivery_cnt", Int32),
                ("c_data", Char),
            ],
        ),
        order_status_customer_statement(StatementId::OrderStatusCustomerById, false),
        order_status_customer_statement(StatementId::OrderStatusCustomerByLast, true),
        query(
            StatementId::OrderStatusLatestOrder,
            &[Int32, Int32, Int32],
            "SELECT o_id AS o_id FROM orders \
             WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3 \
             ORDER BY o_id DESC LIMIT 1;",
            &[("o_id", Int32)],
        ),
        query(
            StatementId::OrderStatusOrder,
            &[Int32, Int32, Int32],
            "SELECT o_id AS o_id, o_entry_d AS o_entry_d, \
             o_carrier_id AS o_carrier_id FROM orders \
             WHERE o_w_id = $1 AND o_d_id = $2 AND o_id = $3;",
            &[
                ("o_id", Int32),
                ("o_entry_d", Char),
                ("o_carrier_id", Int32),
            ],
        ),
        query(
            StatementId::OrderStatusLines,
            &[Int32, Int32, Int32],
            "SELECT ol_number AS ol_number, ol_i_id AS ol_i_id, \
             ol_supply_w_id AS ol_supply_w_id, ol_quantity AS ol_quantity, \
             ol_amount AS ol_amount, ol_delivery_d AS ol_delivery_d \
             FROM order_line \
             WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3 \
             ORDER BY ol_number ASC;",
            &[
                ("ol_number", Int32),
                ("ol_i_id", Int32),
                ("ol_supply_w_id", Int32),
                ("ol_quantity", Int32),
                ("ol_amount", Float32),
                ("ol_delivery_d", Char),
            ],
        ),
        query(
            StatementId::DeliveryOldestOrder,
            &[Int32, Int32],
            "SELECT MIN(no_o_id) AS no_o_id FROM new_orders \
             WHERE no_w_id = $1 AND no_d_id = $2;",
            &[("no_o_id", Int32)],
        ),
        command(
            StatementId::DeliveryLockQueue,
            &[Int32, Int32, Int32],
            "UPDATE new_orders SET no_o_id = no_o_id \
             WHERE no_w_id = $1 AND no_d_id = $2 AND no_o_id = $3;",
        ),
        query(
            StatementId::DeliveryConfirmQueue,
            &[Int32, Int32, Int32],
            "SELECT no_o_id AS no_o_id FROM new_orders \
             WHERE no_w_id = $1 AND no_d_id = $2 AND no_o_id = $3;",
            &[("no_o_id", Int32)],
        ),
        query(
            StatementId::DeliveryOrder,
            &[Int32, Int32, Int32],
            "SELECT o_c_id AS o_c_id FROM orders \
             WHERE o_w_id = $1 AND o_d_id = $2 AND o_id = $3;",
            &[("o_c_id", Int32)],
        ),
        query(
            StatementId::DeliveryCustomer,
            &[Int32, Int32, Int32],
            "SELECT customer.c_id AS c_id, customer.c_balance AS c_balance, \
             customer.c_payment_cnt AS c_payment_cnt, \
             customer.c_delivery_cnt AS c_delivery_cnt \
             FROM customer, orders \
             WHERE orders.o_w_id = $1 AND orders.o_d_id = $2 AND orders.o_id = $3 \
             AND customer.c_w_id = orders.o_w_id \
             AND customer.c_d_id = orders.o_d_id \
             AND customer.c_id = orders.o_c_id;",
            &[
                ("c_id", Int32),
                ("c_balance", Float32),
                ("c_payment_cnt", Int32),
                ("c_delivery_cnt", Int32),
            ],
        ),
        query(
            StatementId::DeliveryLineRows,
            &[Int32, Int32, Int32],
            "SELECT ol_number AS ol_number, ol_amount AS ol_amount \
             FROM order_line \
             WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3 \
             ORDER BY ol_number ASC;",
            &[("ol_number", Int32), ("ol_amount", Float32)],
        ),
        query(
            StatementId::DeliveryLineSum,
            &[Int32, Int32, Int32],
            "SELECT SUM(ol_amount) AS ol_amount_sum FROM order_line \
             WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3;",
            &[("ol_amount_sum", Float32)],
        ),
        command(
            StatementId::DeliveryDeleteQueue,
            &[Int32, Int32, Int32],
            "DELETE FROM new_orders \
             WHERE no_w_id = $1 AND no_d_id = $2 AND no_o_id = $3;",
        ),
        command(
            StatementId::DeliveryUpdateOrder,
            &[Int32, Int32, Int32, Int32],
            "UPDATE orders SET o_carrier_id = $1 \
             WHERE o_w_id = $2 AND o_d_id = $3 AND o_id = $4;",
        ),
        command(
            StatementId::DeliveryUpdateLines,
            &[Char, Int32, Int32, Int32],
            "UPDATE order_line SET ol_delivery_d = $1 \
             WHERE ol_w_id = $2 AND ol_d_id = $3 AND ol_o_id = $4;",
        ),
        command(
            StatementId::DeliveryUpdateCustomer,
            &[Float32, Int32, Int32, Int32],
            "UPDATE customer \
             SET c_balance = c_balance + $1, \
             c_delivery_cnt = c_delivery_cnt + 1 \
             WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4;",
        ),
        query(
            StatementId::DeliveryCustomerAfter,
            &[Int32, Int32, Int32],
            "SELECT c_balance AS c_balance, c_payment_cnt AS c_payment_cnt, \
             c_delivery_cnt AS c_delivery_cnt \
             FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3;",
            &[
                ("c_balance", Float32),
                ("c_payment_cnt", Int32),
                ("c_delivery_cnt", Int32),
            ],
        ),
        query(
            StatementId::StockLevelNextOrder,
            &[Int32, Int32],
            "SELECT d_next_o_id AS d_next_o_id, d_tax AS d_tax FROM district \
             WHERE d_w_id = $1 AND d_id = $2;",
            &[("d_next_o_id", Int32), ("d_tax", Float32)],
        ),
        query(
            StatementId::StockLevelCount,
            &[Int32, Int32, Int32, Int32, Int32],
            "SELECT COUNT(DISTINCT order_line.ol_i_id) AS low_stock_count \
             FROM order_line, stock \
             WHERE order_line.ol_w_id = $1 AND order_line.ol_d_id = $2 \
             AND order_line.ol_o_id >= $3 \
             AND order_line.ol_o_id < $4 \
             AND stock.s_w_id = $1 \
             AND stock.s_i_id = order_line.ol_i_id \
             AND stock.s_quantity < $5;",
            &[("low_stock_count", Int32)],
        ),
    ]
}

/// Render one complete per-run catalogue from the persisted opaque layout.
///
/// The canonical builder remains the logical source of relational semantics;
/// this transformation replaces every statement id, SQL identifier, and
/// declared result alias from the same immutable runtime schema.
pub fn runtime_catalog(schema: &RuntimeSchema) -> Result<Vec<Statement>, CatalogError> {
    schema
        .validate()
        .map_err(|error| CatalogError::new(format!("invalid runtime schema: {error}")))?;
    let mut catalog = final2026_catalog();
    if catalog.len() != StatementId::ALL.len() {
        return Err(CatalogError::new(format!(
            "logical catalogue has {} statements, expected {}",
            catalog.len(),
            StatementId::ALL.len()
        )));
    }
    for (statement, logical_id) in catalog.iter_mut().zip(StatementId::ALL) {
        if statement.id != logical_id.wire_id() {
            return Err(CatalogError::new(format!(
                "logical catalogue order mismatch at {:?}: found canonical id {}",
                logical_id, statement.id
            )));
        }
        statement.id = schema
            .statements()
            .id(logical_id.key())
            .map_err(|error| CatalogError::new(error.to_string()))?;
        statement.sql = schema.render_sql(&statement.sql);
        if let StatementKind::Query { columns } = &mut statement.kind {
            for column in columns {
                column.name = schema.render_sql(&column.name);
            }
        }
    }
    validate_runtime_catalog(&catalog, schema)?;
    Ok(catalog)
}

/// One validated catalogue derived once from the persisted per-run schema.
///
/// Executors share this value across every session so statement ids, SQL, and
/// declared result aliases cannot be regenerated or mixed after dispatch
/// begins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCatalog {
    schema_fingerprint: u64,
    statement_layout: StatementLayout,
    statements: Vec<Statement>,
}

impl RuntimeCatalog {
    pub fn from_schema(schema: &RuntimeSchema) -> Result<Self, CatalogError> {
        let statements = runtime_catalog(schema)?;
        if statements.len() != StatementId::ALL.len() {
            return Err(CatalogError::new(format!(
                "runtime catalogue has {} statements, expected {}",
                statements.len(),
                StatementId::ALL.len()
            )));
        }
        Ok(Self {
            schema_fingerprint: schema.fingerprint(),
            statement_layout: schema.statements().clone(),
            statements,
        })
    }

    pub const fn schema_fingerprint(&self) -> u64 {
        self.schema_fingerprint
    }

    pub fn statement_layout(&self) -> &StatementLayout {
        &self.statement_layout
    }

    pub fn statements(&self) -> &[Statement] {
        &self.statements
    }
}

fn payment_customer_statement(id: StatementId, by_last_name: bool) -> Statement {
    use SqlType::{Char, Float32, Int32};

    let predicate = if by_last_name {
        "customer.c_last = $3 ORDER BY customer.c_first ASC, customer.c_id ASC"
    } else {
        "customer.c_id = $3"
    };
    query(
        id,
        &[Int32, Int32, if by_last_name { Char } else { Int32 }],
        &format!(
            "SELECT c_id AS c_id, c_first AS c_first, c_middle AS c_middle, \
             c_last AS c_last, c_street_1 AS c_street_1, \
             c_street_2 AS c_street_2, c_city AS c_city, c_state AS c_state, \
             c_zip AS c_zip, c_phone AS c_phone, c_since AS c_since, \
             c_credit AS c_credit, c_credit_lim AS c_credit_lim, \
             c_discount AS c_discount, c_balance AS c_balance, \
             c_ytd_payment AS c_ytd_payment, c_payment_cnt AS c_payment_cnt, \
             c_delivery_cnt AS c_delivery_cnt, c_data AS c_data FROM customer \
             WHERE customer.c_w_id = $1 AND customer.c_d_id = $2 AND {predicate};"
        ),
        &[
            ("c_id", Int32),
            ("c_first", Char),
            ("c_middle", Char),
            ("c_last", Char),
            ("c_street_1", Char),
            ("c_street_2", Char),
            ("c_city", Char),
            ("c_state", Char),
            ("c_zip", Char),
            ("c_phone", Char),
            ("c_since", Char),
            ("c_credit", Char),
            ("c_credit_lim", Int32),
            ("c_discount", Float32),
            ("c_balance", Float32),
            ("c_ytd_payment", Float32),
            ("c_payment_cnt", Int32),
            ("c_delivery_cnt", Int32),
            ("c_data", Char),
        ],
    )
}

fn order_status_customer_statement(id: StatementId, by_last_name: bool) -> Statement {
    use SqlType::{Char, Float32, Int32};

    let predicate = if by_last_name {
        "c_last = $3 ORDER BY c_first ASC, c_id ASC"
    } else {
        "c_id = $3"
    };
    query(
        id,
        &[Int32, Int32, if by_last_name { Char } else { Int32 }],
        &format!(
            "SELECT c_id AS c_id, c_balance AS c_balance, c_first AS c_first, \
             c_middle AS c_middle, c_last AS c_last FROM customer \
             WHERE c_w_id = $1 AND c_d_id = $2 AND {predicate};"
        ),
        &[
            ("c_id", Int32),
            ("c_balance", Float32),
            ("c_first", Char),
            ("c_middle", Char),
            ("c_last", Char),
        ],
    )
}

fn command(id: StatementId, params: &[SqlType], sql: &str) -> Statement {
    Statement {
        id: id.wire_id(),
        kind: StatementKind::Command,
        param_types: params.to_vec(),
        sql: sql.to_owned(),
    }
}

fn query(id: StatementId, params: &[SqlType], sql: &str, columns: &[(&str, SqlType)]) -> Statement {
    Statement {
        id: id.wire_id(),
        kind: StatementKind::Query {
            columns: columns
                .iter()
                .map(|(name, sql_type)| Column {
                    name: (*name).to_owned(),
                    sql_type: *sql_type,
                })
                .collect(),
        },
        param_types: params.to_vec(),
        sql: sql.to_owned(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogError {
    detail: String,
}

impl CatalogError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for CatalogError {}

pub fn validate_catalog(catalog: &[Statement]) -> Result<(), CatalogError> {
    validate_catalog_with(catalog, |id| Ok(id.wire_id()))
}

pub fn validate_runtime_catalog(
    catalog: &[Statement],
    schema: &RuntimeSchema,
) -> Result<(), CatalogError> {
    validate_catalog_with(catalog, |id| {
        schema
            .statements()
            .id(id.key())
            .map_err(|error| CatalogError::new(error.to_string()))
    })
}

fn validate_catalog_with(
    catalog: &[Statement],
    resolve_id: impl Fn(StatementId) -> Result<u16, CatalogError>,
) -> Result<(), CatalogError> {
    if catalog.is_empty() || catalog.len() > 256 {
        return Err(CatalogError::new(
            "catalogue must contain between 1 and 256 statements",
        ));
    }

    let mut ids = BTreeSet::new();
    for statement in catalog {
        if statement.id == 0 {
            return Err(CatalogError::new(format!(
                "statement id {} is reserved",
                statement.id
            )));
        }
        if !ids.insert(statement.id) {
            return Err(CatalogError::new(format!(
                "duplicate statement id {}",
                statement.id
            )));
        }
        if statement.sql.is_empty() || statement.sql.as_bytes().contains(&0) {
            return Err(CatalogError::new(format!(
                "statement {} has empty or NUL-containing SQL",
                statement.id
            )));
        }
        let upper = statement.sql.to_ascii_uppercase();
        if upper.contains("OUTPUT_FILE") || upper.contains("SET OUTPUT") {
            return Err(CatalogError::new(format!(
                "statement {} changes output-file state",
                statement.id
            )));
        }

        let ordinals = parameter_ordinals(&statement.sql)?;
        let expected: BTreeSet<usize> = (1..=statement.param_types.len()).collect();
        if ordinals != expected {
            return Err(CatalogError::new(format!(
                "statement {} parameter markers {ordinals:?} do not match dense schema {expected:?}",
                statement.id
            )));
        }

        if let StatementKind::Query { columns } = &statement.kind {
            if columns.is_empty() {
                return Err(CatalogError::new(format!(
                    "query statement {} has no declared columns",
                    statement.id
                )));
            }
            for column in columns {
                let alias = format!(" AS {}", column.name.to_ascii_uppercase());
                if !upper.contains(&alias) {
                    return Err(CatalogError::new(format!(
                        "query statement {} does not project fixed alias {}",
                        statement.id, column.name
                    )));
                }
            }
        }
    }

    validate_stage_templates(catalog, resolve_id)
}

fn parameter_ordinals(sql: &str) -> Result<BTreeSet<usize>, CatalogError> {
    let bytes = sql.as_bytes();
    let mut ordinals = BTreeSet::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'$' {
            index += 1;
            continue;
        }

        let marker_start = index;
        index += 1;
        let digits_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if digits_start == index {
            return Err(CatalogError::new(format!(
                "invalid parameter marker at byte {marker_start}"
            )));
        }
        let ordinal = sql[digits_start..index].parse::<usize>().map_err(|_| {
            CatalogError::new(format!("parameter marker at byte {marker_start} overflows"))
        })?;
        if ordinal == 0 {
            return Err(CatalogError::new("$0 is not a valid parameter marker"));
        }
        ordinals.insert(ordinal);
    }
    Ok(ordinals)
}

fn validate_stage_templates(
    catalog: &[Statement],
    resolve_id: impl Fn(StatementId) -> Result<u16, CatalogError>,
) -> Result<(), CatalogError> {
    let ids: BTreeSet<u16> = catalog.iter().map(|statement| statement.id).collect();
    let groups: &[&[StageTemplate]] = &[
        NEW_ORDER_STAGES,
        PAYMENT_STAGES,
        ORDER_STATUS_STAGES,
        DELIVERY_STAGES,
        STOCK_LEVEL_STAGES,
    ];

    for stages in groups {
        for (index, stage) in stages.iter().enumerate() {
            if stage.round_trip as usize != index + 1 {
                return Err(CatalogError::new(
                    "stage round trips must be dense and one-based",
                ));
            }
            if stage.steps.is_empty() {
                return Err(CatalogError::new("stage must contain at least one step"));
            }
            for step in stage.steps {
                if step.alternatives.is_empty() {
                    return Err(CatalogError::new(
                        "stage step must contain at least one alternative",
                    ));
                }
                for statement_id in step.alternatives {
                    let runtime_id = resolve_id(*statement_id)?;
                    if !ids.contains(&runtime_id) {
                        return Err(CatalogError::new(format!(
                            "stage references absent statement {}",
                            runtime_id
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod runtime_tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn wrapped_stock_matches_public_conditional_shape() {
        let statement = final2026_catalog()
            .into_iter()
            .find(|statement| statement.id == StatementId::NewOrderUpdateStockWrapped.wire_id())
            .unwrap();

        assert_eq!(statement.param_types.len(), 6);
        assert!(statement
            .sql
            .contains("s_quantity = s_quantity - $1 + 91"));
        assert!(statement.sql.contains("s_quantity < $6"));
        assert_eq!(
            parameter_ordinals(&statement.sql).unwrap(),
            BTreeSet::from([1, 2, 3, 4, 5, 6])
        );
    }

    #[test]
    fn stock_level_binds_both_order_bounds_without_sql_arithmetic() {
        let statement = final2026_catalog()
            .into_iter()
            .find(|statement| statement.id == StatementId::StockLevelCount.wire_id())
            .unwrap();

        assert_eq!(statement.param_types.len(), 5);
        assert!(statement.sql.contains("ol_o_id >= $3"));
        assert!(statement.sql.contains("ol_o_id < $4"));
        assert!(statement.sql.contains("s_quantity < $5"));
        assert!(!statement.sql.contains("$3 - 20"));
        assert_eq!(
            parameter_ordinals(&statement.sql).unwrap(),
            BTreeSet::from([1, 2, 3, 4, 5])
        );
    }

    #[test]
    fn opaque_catalog_is_seed_specific_and_stable_across_32_sessions() {
        let first = RuntimeSchema::opaque(73).unwrap();
        let second = RuntimeSchema::opaque(74).unwrap();
        let first_catalog = runtime_catalog(&first).unwrap();
        let second_catalog = runtime_catalog(&second).unwrap();
        assert_ne!(
            first_catalog
                .iter()
                .map(|statement| statement.id)
                .collect::<Vec<_>>(),
            second_catalog
                .iter()
                .map(|statement| statement.id)
                .collect::<Vec<_>>()
        );

        for schema in [&first, &second] {
            let expected = runtime_catalog(schema).unwrap();
            let ids = expected
                .iter()
                .map(|statement| statement.id)
                .collect::<BTreeSet<_>>();
            assert_eq!(ids.len(), StatementId::ALL.len());
            assert!(!ids.contains(&0));
            for session in 0..32 {
                assert_eq!(
                    runtime_catalog(schema).unwrap(),
                    expected,
                    "session {session} diverged from its run catalogue"
                );
            }
        }
    }

    #[test]
    fn opaque_catalog_renders_sql_and_declared_aliases_together() {
        let schema = RuntimeSchema::opaque(2026).unwrap();
        let runtime = RuntimeCatalog::from_schema(&schema).unwrap();
        let catalog = runtime.statements();
        let payment_id = schema
            .statements()
            .id(StatementId::PaymentWarehouse.key())
            .unwrap();
        let payment = catalog
            .iter()
            .find(|statement| statement.id == payment_id)
            .unwrap();
        assert_eq!(payment.id, payment_id);
        assert!(payment
            .sql
            .contains(schema.table(crate::runtime_schema::LogicalTable::Warehouse)));
        assert!(!payment.sql.contains(" FROM warehouse "));
        validate_runtime_catalog(&catalog, &schema).unwrap();
        assert_eq!(runtime.schema_fingerprint(), schema.fingerprint());
        assert_eq!(runtime.statement_layout(), schema.statements());
    }
}
