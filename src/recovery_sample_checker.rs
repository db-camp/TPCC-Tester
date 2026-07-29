//! Bounded typed recovery probes backed only by sealed terminal evidence.
//!
//! Every probe is one anonymous-schema `EXEC_STREAM` query. Point-query rows
//! are folded into a fixed expected set, so neither a large response nor a
//! server diagnostic can become part of the tester's retained state or error
//! output.

use crate::connection::client::RmdbClient;
use crate::connection::wire::{
    Column, FoldStreamResponse, SqlType, WireError, WireResult, WireValue,
};
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;
use crate::ranking::evidence_collector::SealedIntervalEvidence;
use crate::ranking::rich_recovery_samples::{
    HistoryGroupKey, SealedBadCreditCustomerSample, SealedDeliverySample, SealedHistoryGroup,
    SealedNewOrderSample, SealedRichRecoverySamples,
};
use crate::ranking::terminal_evidence::{validate_terminal_evidence, TerminalEvidenceView};
use crate::runtime_schema::RuntimeSchema;

const MAX_EXACT_POINT_ROWS: usize = 15;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SampleDomain {
    NewOrder,
    Delivery,
    Stock,
    Customer,
    BadCredit,
    History,
}

impl SampleDomain {
    const fn label(self) -> &'static str {
        match self {
            Self::NewOrder => "new-order",
            Self::Delivery => "delivery",
            Self::Stock => "stock",
            Self::Customer => "customer",
            Self::BadCredit => "bad-credit",
            Self::History => "history",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueryScope {
    domain: SampleDomain,
    ordinal: usize,
}

impl QueryScope {
    const fn new(domain: SampleDomain, ordinal: usize) -> Self {
        Self { domain, ordinal }
    }

    fn message(self, category: &str) -> String {
        format!(
            "recovery {} sample query {} {category}",
            self.domain.label(),
            self.ordinal
        )
    }
}

#[derive(Debug)]
struct ExpectedPointQuery {
    scope: QueryScope,
    logical_sql: String,
    column_types: Vec<SqlType>,
    expected_rows: Vec<Vec<WireValue>>,
}

impl ExpectedPointQuery {
    fn new(
        scope: QueryScope,
        logical_sql: String,
        column_types: Vec<SqlType>,
        expected_rows: Vec<Vec<WireValue>>,
    ) -> Result<Self, TpccError> {
        if column_types.is_empty() || expected_rows.len() > MAX_EXACT_POINT_ROWS {
            return Err(TpccError::Protocol(
                scope.message("has invalid bounded expectations"),
            ));
        }
        for (index, row) in expected_rows.iter().enumerate() {
            if row.len() != column_types.len()
                || row
                    .iter()
                    .zip(&column_types)
                    .any(|(value, sql_type)| !wire_value_has_type(value, *sql_type))
            {
                return Err(TpccError::Protocol(
                    scope.message("has invalid typed expectations"),
                ));
            }
            if expected_rows[..index].contains(row) {
                return Err(TpccError::Protocol(
                    scope.message("has duplicate expected rows"),
                ));
            }
        }
        Ok(Self {
            scope,
            logical_sql,
            column_types,
            expected_rows,
        })
    }
}

#[derive(Debug)]
struct ExactRowsFold {
    expected_types: Vec<SqlType>,
    unmatched_rows: Vec<Vec<WireValue>>,
    expected_row_count: u64,
    saw_meta: bool,
}

impl ExactRowsFold {
    fn new(query: &ExpectedPointQuery) -> Self {
        Self {
            expected_types: query.column_types.clone(),
            unmatched_rows: query.expected_rows.clone(),
            expected_row_count: query.expected_rows.len() as u64,
            saw_meta: false,
        }
    }

    fn accept_meta(&mut self, columns: &[Column]) -> WireResult<()> {
        if columns.len() != self.expected_types.len()
            || columns
                .iter()
                .zip(&self.expected_types)
                .any(|(column, expected)| column.sql_type != *expected)
        {
            return Err(WireError::Protocol(
                "recovery sample metadata mismatch".to_owned(),
            ));
        }
        self.saw_meta = true;
        Ok(())
    }

    fn accept_row(&mut self, row: Vec<WireValue>) -> WireResult<()> {
        if !self.saw_meta
            || row.len() != self.expected_types.len()
            || row
                .iter()
                .zip(&self.expected_types)
                .any(|(value, expected)| !wire_value_has_type(value, *expected))
        {
            return Err(WireError::Protocol(
                "recovery sample row type mismatch".to_owned(),
            ));
        }
        let Some(index) = self
            .unmatched_rows
            .iter()
            .position(|expected| expected == &row)
        else {
            return Err(WireError::Protocol(
                "recovery sample row mismatch".to_owned(),
            ));
        };
        self.unmatched_rows.swap_remove(index);
        Ok(())
    }

    fn finish(self, row_count: u64) -> WireResult<()> {
        if !self.saw_meta || row_count != self.expected_row_count || !self.unmatched_rows.is_empty()
        {
            return Err(WireError::Protocol(
                "recovery sample result cardinality mismatch".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
struct NewOrderLineExpected {
    number: i32,
    item_id: i32,
    supply_warehouse: i32,
    delivery_timestamp: Vec<u8>,
    quantity: i32,
    amount_bits: u32,
    district_info: Vec<u8>,
}

#[derive(Debug, Eq, PartialEq)]
struct NewOrderExpected {
    warehouse_id: i32,
    district_id: i32,
    order_id: i32,
    customer_id: i32,
    entry_timestamp: Vec<u8>,
    carrier_id: i32,
    line_count: i32,
    all_local: i32,
    queue_present: bool,
    lines: Vec<NewOrderLineExpected>,
}

impl NewOrderExpected {
    fn from_sealed(sample: &SealedNewOrderSample, ordinal: usize) -> Result<Self, TpccError> {
        let scope = QueryScope::new(SampleDomain::NewOrder, ordinal);
        let key = sample.key();
        let lines = sample
            .lines()
            .iter()
            .map(|line| {
                Ok(NewOrderLineExpected {
                    number: i32::from(line.number()),
                    item_id: i32::try_from(line.item_id()).map_err(|_| {
                        TpccError::Protocol(scope.message("has invalid sealed evidence"))
                    })?,
                    supply_warehouse: i32::from(line.supply_warehouse()),
                    delivery_timestamp: line.delivery_timestamp().to_vec(),
                    quantity: i32::from(line.quantity()),
                    amount_bits: line.amount_bits(),
                    district_info: line.district_info().to_vec(),
                })
            })
            .collect::<Result<Vec<_>, TpccError>>()?;
        if lines.len() != usize::from(sample.line_count()) {
            return Err(TpccError::Protocol(
                scope.message("has invalid sealed evidence"),
            ));
        }
        Ok(Self {
            warehouse_id: i32::from(key.warehouse_id()),
            district_id: i32::from(key.district_id()),
            order_id: key.order_id(),
            customer_id: i32::from(sample.customer_id()),
            entry_timestamp: sample.entry_timestamp().to_vec(),
            carrier_id: i32::from(sample.carrier_id()),
            line_count: i32::from(sample.line_count()),
            all_local: i32::from(sample.all_local()),
            queue_present: sample.queue_present(),
            lines,
        })
    }
}

async fn check_new_order_samples(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    rich: &SealedRichRecoverySamples,
) -> Result<(), TpccError> {
    require_nonempty_samples(
        SampleDomain::NewOrder,
        rich.new_order_commit_count(),
        rich.new_orders().len(),
    )?;
    for (sample_index, sample) in rich.new_orders().iter().enumerate() {
        let base_ordinal = checked_query_ordinal(SampleDomain::NewOrder, sample_index, 3)?;
        let expected = NewOrderExpected::from_sealed(sample, base_ordinal)?;
        for query in new_order_point_queries(&expected, base_ordinal)? {
            execute_exact_point_query(client, schema, query).await?;
        }
    }
    Ok(())
}

fn new_order_point_queries(
    sample: &NewOrderExpected,
    base_ordinal: usize,
) -> Result<Vec<ExpectedPointQuery>, TpccError> {
    let orders_scope = QueryScope::new(SampleDomain::NewOrder, base_ordinal);
    let queue_scope = QueryScope::new(SampleDomain::NewOrder, base_ordinal + 1);
    let lines_scope = QueryScope::new(SampleDomain::NewOrder, base_ordinal + 2);
    let key_predicate = format!(
        "orders.o_w_id = {} AND orders.o_d_id = {} AND orders.o_id = {}",
        sample.warehouse_id, sample.district_id, sample.order_id
    );
    let order = ExpectedPointQuery::new(
        orders_scope,
        format!(
            "SELECT orders.o_id, orders.o_d_id, orders.o_w_id, orders.o_c_id, orders.o_entry_d, orders.o_carrier_id, orders.o_ol_cnt, orders.o_all_local FROM orders WHERE {key_predicate}"
        ),
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Char,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
        ],
        vec![vec![
            WireValue::Int32(sample.order_id),
            WireValue::Int32(sample.district_id),
            WireValue::Int32(sample.warehouse_id),
            WireValue::Int32(sample.customer_id),
            WireValue::Char(sample.entry_timestamp.clone()),
            WireValue::Int32(sample.carrier_id),
            WireValue::Int32(sample.line_count),
            WireValue::Int32(sample.all_local),
        ]],
    )?;
    let queue_rows = sample
        .queue_present
        .then(|| {
            vec![
                WireValue::Int32(sample.order_id),
                WireValue::Int32(sample.district_id),
                WireValue::Int32(sample.warehouse_id),
            ]
        })
        .into_iter()
        .collect();
    let queue = ExpectedPointQuery::new(
        queue_scope,
        format!(
            "SELECT new_orders.no_o_id, new_orders.no_d_id, new_orders.no_w_id FROM new_orders WHERE new_orders.no_w_id = {} AND new_orders.no_d_id = {} AND new_orders.no_o_id = {}",
            sample.warehouse_id, sample.district_id, sample.order_id
        ),
        vec![SqlType::Int32, SqlType::Int32, SqlType::Int32],
        queue_rows,
    )?;
    let line_rows = sample
        .lines
        .iter()
        .map(|line| {
            vec![
                WireValue::Int32(sample.order_id),
                WireValue::Int32(sample.district_id),
                WireValue::Int32(sample.warehouse_id),
                WireValue::Int32(line.number),
                WireValue::Int32(line.item_id),
                WireValue::Int32(line.supply_warehouse),
                WireValue::Char(line.delivery_timestamp.clone()),
                WireValue::Int32(line.quantity),
                WireValue::Float32(line.amount_bits),
                WireValue::Char(line.district_info.clone()),
            ]
        })
        .collect();
    let lines = ExpectedPointQuery::new(
        lines_scope,
        format!(
            "SELECT order_line.ol_o_id, order_line.ol_d_id, order_line.ol_w_id, order_line.ol_number, order_line.ol_i_id, order_line.ol_supply_w_id, order_line.ol_delivery_d, order_line.ol_quantity, order_line.ol_amount, order_line.ol_dist_info FROM order_line WHERE order_line.ol_w_id = {} AND order_line.ol_d_id = {} AND order_line.ol_o_id = {}",
            sample.warehouse_id, sample.district_id, sample.order_id
        ),
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Char,
            SqlType::Int32,
            SqlType::Float32,
            SqlType::Char,
        ],
        line_rows,
    )?;
    Ok(vec![order, queue, lines])
}

#[derive(Debug, Eq, PartialEq)]
struct DeliveryLineExpected {
    number: i32,
    delivery_timestamp: Vec<u8>,
    amount_bits: u32,
}

#[derive(Debug, Eq, PartialEq)]
struct DeliveryExpected {
    warehouse_id: i32,
    district_id: i32,
    order_id: i32,
    customer_id: i32,
    carrier_id: i32,
    queue_present: bool,
    delivery_timestamp: Vec<u8>,
    lines: Vec<DeliveryLineExpected>,
}

impl DeliveryExpected {
    fn from_sealed(sample: &SealedDeliverySample, ordinal: usize) -> Result<Self, TpccError> {
        let scope = QueryScope::new(SampleDomain::Delivery, ordinal);
        let key = sample.key();
        let delivery_timestamp = sample.delivery_timestamp().to_vec();
        let lines = sample
            .lines()
            .iter()
            .map(|line| {
                if line.delivery_timestamp() != delivery_timestamp {
                    return Err(TpccError::Protocol(
                        scope.message("has invalid sealed evidence"),
                    ));
                }
                Ok(DeliveryLineExpected {
                    number: i32::from(line.number()),
                    delivery_timestamp: line.delivery_timestamp().to_vec(),
                    amount_bits: line.amount_bits(),
                })
            })
            .collect::<Result<Vec<_>, TpccError>>()?;
        if sample.queue_present() || lines.is_empty() || lines.len() > MAX_EXACT_POINT_ROWS {
            return Err(TpccError::Protocol(
                scope.message("has invalid sealed evidence"),
            ));
        }
        Ok(Self {
            warehouse_id: i32::from(key.warehouse_id()),
            district_id: i32::from(key.district_id()),
            order_id: key.order_id(),
            customer_id: sample.customer_id(),
            carrier_id: i32::from(sample.carrier_id()),
            queue_present: sample.queue_present(),
            delivery_timestamp,
            lines,
        })
    }
}

async fn check_delivery_samples(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    rich: &SealedRichRecoverySamples,
) -> Result<(), TpccError> {
    require_nonempty_samples(
        SampleDomain::Delivery,
        rich.delivered_order_count(),
        rich.deliveries().len(),
    )?;
    for (sample_index, sample) in rich.deliveries().iter().enumerate() {
        let base_ordinal = checked_query_ordinal(SampleDomain::Delivery, sample_index, 3)?;
        let expected = DeliveryExpected::from_sealed(sample, base_ordinal)?;
        for query in delivery_point_queries(&expected, base_ordinal)? {
            execute_exact_point_query(client, schema, query).await?;
        }
    }
    Ok(())
}

fn delivery_point_queries(
    sample: &DeliveryExpected,
    base_ordinal: usize,
) -> Result<Vec<ExpectedPointQuery>, TpccError> {
    let orders_scope = QueryScope::new(SampleDomain::Delivery, base_ordinal);
    let queue_scope = QueryScope::new(SampleDomain::Delivery, base_ordinal + 1);
    let lines_scope = QueryScope::new(SampleDomain::Delivery, base_ordinal + 2);
    if sample.queue_present
        || sample.lines.is_empty()
        || sample
            .lines
            .iter()
            .any(|line| line.delivery_timestamp != sample.delivery_timestamp)
    {
        return Err(TpccError::Protocol(
            orders_scope.message("has invalid sealed evidence"),
        ));
    }
    let order = ExpectedPointQuery::new(
        orders_scope,
        format!(
            "SELECT orders.o_id, orders.o_d_id, orders.o_w_id, orders.o_c_id, orders.o_carrier_id, orders.o_ol_cnt FROM orders WHERE orders.o_w_id = {} AND orders.o_d_id = {} AND orders.o_id = {}",
            sample.warehouse_id, sample.district_id, sample.order_id
        ),
        vec![SqlType::Int32; 6],
        vec![vec![
            WireValue::Int32(sample.order_id),
            WireValue::Int32(sample.district_id),
            WireValue::Int32(sample.warehouse_id),
            WireValue::Int32(sample.customer_id),
            WireValue::Int32(sample.carrier_id),
            WireValue::Int32(sample.lines.len() as i32),
        ]],
    )?;
    let queue = ExpectedPointQuery::new(
        queue_scope,
        format!(
            "SELECT new_orders.no_o_id, new_orders.no_d_id, new_orders.no_w_id FROM new_orders WHERE new_orders.no_w_id = {} AND new_orders.no_d_id = {} AND new_orders.no_o_id = {}",
            sample.warehouse_id, sample.district_id, sample.order_id
        ),
        vec![SqlType::Int32, SqlType::Int32, SqlType::Int32],
        Vec::new(),
    )?;
    let line_rows = sample
        .lines
        .iter()
        .map(|line| {
            vec![
                WireValue::Int32(sample.order_id),
                WireValue::Int32(sample.district_id),
                WireValue::Int32(sample.warehouse_id),
                WireValue::Int32(line.number),
                WireValue::Char(line.delivery_timestamp.clone()),
                WireValue::Float32(line.amount_bits),
            ]
        })
        .collect();
    let lines = ExpectedPointQuery::new(
        lines_scope,
        format!(
            "SELECT order_line.ol_o_id, order_line.ol_d_id, order_line.ol_w_id, order_line.ol_number, order_line.ol_delivery_d, order_line.ol_amount FROM order_line WHERE order_line.ol_w_id = {} AND order_line.ol_d_id = {} AND order_line.ol_o_id = {}",
            sample.warehouse_id, sample.district_id, sample.order_id
        ),
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Char,
            SqlType::Float32,
        ],
        line_rows,
    )?;
    Ok(vec![order, queue, lines])
}

#[derive(Debug, Eq, PartialEq)]
struct StockExpected {
    warehouse_id: i32,
    item_id: i32,
    quantity: i32,
    ytd_bits: u32,
    order_count: i32,
    remote_count: i32,
}

async fn check_stock_samples(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    intervals: &SealedIntervalEvidence,
) -> Result<(), TpccError> {
    require_nonempty_samples(
        SampleDomain::Stock,
        intervals.stock_update_count(),
        intervals.stock_sample_count(),
    )?;
    for (sample_index, sample) in intervals.stocks().enumerate() {
        let ordinal = checked_query_ordinal(SampleDomain::Stock, sample_index, 1)?;
        let key = sample.key();
        let endpoint = sample.endpoint();
        let expected = StockExpected {
            warehouse_id: key.warehouse_id,
            item_id: key.item_id,
            quantity: endpoint.quantity,
            ytd_bits: endpoint.ytd_bits,
            order_count: endpoint.order_count,
            remote_count: endpoint.remote_count,
        };
        execute_exact_point_query(client, schema, stock_point_query(&expected, ordinal)?).await?;
    }
    Ok(())
}

fn stock_point_query(
    stock: &StockExpected,
    ordinal: usize,
) -> Result<ExpectedPointQuery, TpccError> {
    ExpectedPointQuery::new(
        QueryScope::new(SampleDomain::Stock, ordinal),
        format!(
            "SELECT stock.s_w_id, stock.s_i_id, stock.s_quantity, stock.s_ytd, stock.s_order_cnt, stock.s_remote_cnt FROM stock WHERE stock.s_w_id = {} AND stock.s_i_id = {}",
            stock.warehouse_id, stock.item_id
        ),
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Float32,
            SqlType::Int32,
            SqlType::Int32,
        ],
        vec![vec![
            WireValue::Int32(stock.warehouse_id),
            WireValue::Int32(stock.item_id),
            WireValue::Int32(stock.quantity),
            WireValue::Float32(stock.ytd_bits),
            WireValue::Int32(stock.order_count),
            WireValue::Int32(stock.remote_count),
        ]],
    )
}

#[derive(Debug, Eq, PartialEq)]
struct CustomerExpected {
    warehouse_id: i32,
    district_id: i32,
    customer_id: i32,
    credit: Vec<u8>,
    balance_bits: u32,
    ytd_payment_bits: u32,
    payment_count: i32,
    delivery_count: i32,
}

async fn check_customer_samples(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    intervals: &SealedIntervalEvidence,
) -> Result<(), TpccError> {
    require_nonempty_samples(
        SampleDomain::Customer,
        intervals.customer_update_count(),
        intervals.customer_sample_count(),
    )?;
    let generator =
        TpccDataGen::with_seed(i32::from(intervals.warehouses()), intervals.sample_seed());
    for (sample_index, sample) in intervals.customers().enumerate() {
        let ordinal = checked_query_ordinal(SampleDomain::Customer, sample_index, 1)?;
        let scope = QueryScope::new(SampleDomain::Customer, ordinal);
        let key = sample.key();
        let endpoint = sample.endpoint();
        let profile = generator
            .initial_customer_profile(key.warehouse_id, key.district_id, key.customer_id)
            .ok_or_else(|| TpccError::Protocol(scope.message("has invalid setup-root evidence")))?;
        let expected = CustomerExpected {
            warehouse_id: key.warehouse_id,
            district_id: key.district_id,
            customer_id: key.customer_id,
            credit: profile.credit().to_vec(),
            balance_bits: endpoint.balance_bits,
            ytd_payment_bits: endpoint.ytd_payment_bits,
            payment_count: endpoint.version.payment_count,
            delivery_count: endpoint.version.delivery_count,
        };
        execute_exact_point_query(client, schema, customer_point_query(&expected, ordinal)?)
            .await?;
    }
    Ok(())
}

fn customer_point_query(
    customer: &CustomerExpected,
    ordinal: usize,
) -> Result<ExpectedPointQuery, TpccError> {
    ExpectedPointQuery::new(
        QueryScope::new(SampleDomain::Customer, ordinal),
        format!(
            "SELECT customer.c_w_id, customer.c_d_id, customer.c_id, customer.c_credit, customer.c_balance, customer.c_ytd_payment, customer.c_payment_cnt, customer.c_delivery_cnt FROM customer WHERE customer.c_w_id = {} AND customer.c_d_id = {} AND customer.c_id = {}",
            customer.warehouse_id, customer.district_id, customer.customer_id
        ),
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Char,
            SqlType::Float32,
            SqlType::Float32,
            SqlType::Int32,
            SqlType::Int32,
        ],
        vec![vec![
            WireValue::Int32(customer.warehouse_id),
            WireValue::Int32(customer.district_id),
            WireValue::Int32(customer.customer_id),
            WireValue::Char(customer.credit.clone()),
            WireValue::Float32(customer.balance_bits),
            WireValue::Float32(customer.ytd_payment_bits),
            WireValue::Int32(customer.payment_count),
            WireValue::Int32(customer.delivery_count),
        ]],
    )
}

#[derive(Debug, Eq, PartialEq)]
struct BadCreditExpected {
    warehouse_id: i32,
    district_id: i32,
    customer_id: i32,
    credit: Vec<u8>,
    payment_count: i32,
    data: Vec<u8>,
}

impl BadCreditExpected {
    fn from_sealed(
        sample: &SealedBadCreditCustomerSample,
        ordinal: usize,
    ) -> Result<Self, TpccError> {
        let scope = QueryScope::new(SampleDomain::BadCredit, ordinal);
        let key = sample.customer_key();
        let expected_updates = sample
            .final_payment_count()
            .checked_sub(1)
            .and_then(|count| u64::try_from(count).ok());
        if expected_updates != Some(sample.committed_payment_updates()) {
            return Err(TpccError::Protocol(
                scope.message("has invalid sealed evidence"),
            ));
        }
        Ok(Self {
            warehouse_id: key.warehouse_id,
            district_id: key.district_id,
            customer_id: key.customer_id,
            credit: sample.expected_credit().to_vec(),
            payment_count: sample.final_payment_count(),
            data: sample.final_data().to_vec(),
        })
    }
}

async fn check_bad_credit_samples(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    rich: &SealedRichRecoverySamples,
) -> Result<(), TpccError> {
    require_nonempty_samples(
        SampleDomain::BadCredit,
        rich.bad_credit_payment_count(),
        rich.bad_credit_customers().len(),
    )?;
    for (sample_index, sample) in rich.bad_credit_customers().iter().enumerate() {
        let ordinal = checked_query_ordinal(SampleDomain::BadCredit, sample_index, 1)?;
        let expected = BadCreditExpected::from_sealed(sample, ordinal)?;
        execute_exact_point_query(client, schema, bad_credit_point_query(&expected, ordinal)?)
            .await?;
    }
    Ok(())
}

fn bad_credit_point_query(
    customer: &BadCreditExpected,
    ordinal: usize,
) -> Result<ExpectedPointQuery, TpccError> {
    ExpectedPointQuery::new(
        QueryScope::new(SampleDomain::BadCredit, ordinal),
        format!(
            "SELECT customer.c_w_id, customer.c_d_id, customer.c_id, customer.c_credit, customer.c_payment_cnt, customer.c_data FROM customer WHERE customer.c_w_id = {} AND customer.c_d_id = {} AND customer.c_id = {}",
            customer.warehouse_id, customer.district_id, customer.customer_id
        ),
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Char,
            SqlType::Int32,
            SqlType::Char,
        ],
        vec![vec![
            WireValue::Int32(customer.warehouse_id),
            WireValue::Int32(customer.district_id),
            WireValue::Int32(customer.customer_id),
            WireValue::Char(customer.credit.clone()),
            WireValue::Int32(customer.payment_count),
            WireValue::Char(customer.data.clone()),
        ]],
    )
}

#[derive(Debug, Eq, PartialEq)]
struct HistoryTupleExpected {
    timestamp: Vec<u8>,
    amount_bits: u32,
    data: Vec<u8>,
    multiplicity: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HistoryPointKey {
    customer_id: i32,
    customer_district_id: i32,
    customer_warehouse_id: i32,
    home_district_id: i32,
    home_warehouse_id: i32,
}

impl From<HistoryGroupKey> for HistoryPointKey {
    fn from(key: HistoryGroupKey) -> Self {
        Self {
            customer_id: key.customer_id(),
            customer_district_id: i32::from(key.customer_district_id()),
            customer_warehouse_id: i32::from(key.customer_warehouse_id()),
            home_district_id: i32::from(key.home_district_id()),
            home_warehouse_id: i32::from(key.home_warehouse_id()),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct HistoryGroupExpected {
    key: HistoryPointKey,
    tuples: Vec<HistoryTupleExpected>,
}

impl HistoryGroupExpected {
    fn from_sealed(group: &SealedHistoryGroup, ordinal: usize) -> Result<Self, TpccError> {
        let scope = QueryScope::new(SampleDomain::History, ordinal);
        if group.tuples().is_empty() || group.tuples().len() > 2 {
            return Err(TpccError::Protocol(
                scope.message("has invalid sealed evidence"),
            ));
        }
        let mut tuples = Vec::with_capacity(group.tuples().len());
        for tuple in group.tuples() {
            let expected = HistoryTupleExpected {
                timestamp: tuple.timestamp().to_vec(),
                amount_bits: tuple.amount_bits(),
                data: tuple.data().to_vec(),
                multiplicity: tuple.expected_total_multiplicity().map_err(|_| {
                    TpccError::Protocol(scope.message("has invalid sealed evidence"))
                })?,
            };
            if expected.multiplicity == 0
                || tuples.iter().any(|retained: &HistoryTupleExpected| {
                    retained.timestamp == expected.timestamp
                        && retained.amount_bits == expected.amount_bits
                        && retained.data == expected.data
                })
            {
                return Err(TpccError::Protocol(
                    scope.message("has invalid sealed evidence"),
                ));
            }
            tuples.push(expected);
        }
        Ok(Self {
            key: group.key().into(),
            tuples,
        })
    }
}

#[derive(Debug)]
struct HistoryGroupFold {
    key: HistoryPointKey,
    retained: Vec<HistoryTupleExpected>,
    saw_meta: bool,
}

impl HistoryGroupFold {
    fn new(group: HistoryGroupExpected) -> Self {
        Self {
            key: group.key,
            retained: group.tuples,
            saw_meta: false,
        }
    }

    fn accept_meta(&mut self, columns: &[Column]) -> WireResult<()> {
        const EXPECTED_TYPES: [SqlType; 8] = [
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Char,
            SqlType::Float32,
            SqlType::Char,
        ];
        if columns.len() != EXPECTED_TYPES.len()
            || columns
                .iter()
                .zip(EXPECTED_TYPES)
                .any(|(column, expected)| column.sql_type != expected)
        {
            return Err(WireError::Protocol(
                "recovery history metadata mismatch".to_owned(),
            ));
        }
        self.saw_meta = true;
        Ok(())
    }

    fn accept_row(&mut self, row: Vec<WireValue>) -> WireResult<()> {
        if !self.saw_meta || row.len() != 8 {
            return Err(WireError::Protocol(
                "recovery history row type mismatch".to_owned(),
            ));
        }
        let [WireValue::Int32(customer_id), WireValue::Int32(customer_district_id), WireValue::Int32(customer_warehouse_id), WireValue::Int32(home_district_id), WireValue::Int32(home_warehouse_id), WireValue::Char(timestamp), WireValue::Float32(amount_bits), WireValue::Char(data)] =
            row.as_slice()
        else {
            return Err(WireError::Protocol(
                "recovery history row type mismatch".to_owned(),
            ));
        };
        if *customer_id != self.key.customer_id
            || *customer_district_id != self.key.customer_district_id
            || *customer_warehouse_id != self.key.customer_warehouse_id
            || *home_district_id != self.key.home_district_id
            || *home_warehouse_id != self.key.home_warehouse_id
        {
            return Err(WireError::Protocol(
                "recovery history group mismatch".to_owned(),
            ));
        }
        if let Some(retained) = self.retained.iter_mut().find(|retained| {
            retained.timestamp.as_slice() == timestamp.as_slice()
                && retained.amount_bits == *amount_bits
                && retained.data.as_slice() == data.as_slice()
        }) {
            retained.multiplicity = retained.multiplicity.checked_sub(1).ok_or_else(|| {
                WireError::Protocol("recovery history tuple multiplicity mismatch".to_owned())
            })?;
        }
        Ok(())
    }

    fn finish(self) -> WireResult<()> {
        if !self.saw_meta
            || self
                .retained
                .iter()
                .any(|retained| retained.multiplicity != 0)
        {
            return Err(WireError::Protocol(
                "recovery history tuple multiplicity mismatch".to_owned(),
            ));
        }
        Ok(())
    }
}

async fn check_history_samples(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    rich: &SealedRichRecoverySamples,
) -> Result<(), TpccError> {
    require_nonempty_samples(
        SampleDomain::History,
        rich.committed_history_row_count(),
        rich.history_groups().len(),
    )?;
    let mut prior_key = None;
    for (sample_index, group) in rich.history_groups().iter().enumerate() {
        let ordinal = checked_query_ordinal(SampleDomain::History, sample_index, 1)?;
        if prior_key.is_some_and(|key| key >= group.key()) {
            return Err(TpccError::Protocol(
                QueryScope::new(SampleDomain::History, ordinal)
                    .message("has invalid sealed evidence"),
            ));
        }
        prior_key = Some(group.key());
        let expected = HistoryGroupExpected::from_sealed(group, ordinal)?;
        execute_history_group_query(client, schema, expected, ordinal).await?;
    }
    Ok(())
}

async fn execute_history_group_query(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    expected: HistoryGroupExpected,
    ordinal: usize,
) -> Result<(), TpccError> {
    let scope = QueryScope::new(SampleDomain::History, ordinal);
    let logical_sql = history_group_sql(expected.key);
    let rendered = render_and_only_point_sql(schema, scope, &logical_sql)?;
    let response = client
        .exec_stream_fold(
            &rendered,
            HistoryGroupFold::new(expected),
            |columns, state| state.accept_meta(columns),
            |_, row, state| state.accept_row(row),
        )
        .await
        .map_err(|error| sanitize_exec_error(scope, error))?;
    finish_history_response(scope, response)
}

fn finish_history_response(
    scope: QueryScope,
    response: FoldStreamResponse<HistoryGroupFold>,
) -> Result<(), TpccError> {
    match response {
        FoldStreamResponse::Query { state, .. } => state
            .finish()
            .map_err(|_| TpccError::Protocol(scope.message("result mismatch"))),
        FoldStreamResponse::CommandOk => Err(TpccError::Protocol(
            scope.message("returned a non-query terminal"),
        )),
        FoldStreamResponse::TransactionAbort { diagnostic: _ } => {
            Err(TpccError::Abort(scope.message("aborted")))
        }
        FoldStreamResponse::Error { diagnostic: _ } => {
            Err(TpccError::QueryError(scope.message("failed")))
        }
    }
}

fn history_group_sql(key: HistoryPointKey) -> String {
    format!(
        "SELECT history.h_c_id, history.h_c_d_id, history.h_c_w_id, history.h_d_id, history.h_w_id, history.h_date, history.h_amount, history.h_data FROM history WHERE history.h_c_id = {} AND history.h_c_d_id = {} AND history.h_c_w_id = {} AND history.h_d_id = {} AND history.h_w_id = {}",
        key.customer_id,
        key.customer_district_id,
        key.customer_warehouse_id,
        key.home_district_id,
        key.home_warehouse_id
    )
}

pub(crate) async fn check_recovery_samples(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    evidence: &dyn TerminalEvidenceView,
) -> Result<(), TpccError> {
    validate_terminal_evidence(evidence).map_err(|_| {
        TpccError::Protocol("recovery terminal evidence failed validation".to_owned())
    })?;
    check_new_order_samples(client, schema, evidence.rich()).await?;
    check_delivery_samples(client, schema, evidence.rich()).await?;
    check_stock_samples(client, schema, evidence.intervals()).await?;
    check_customer_samples(client, schema, evidence.intervals()).await?;
    check_bad_credit_samples(client, schema, evidence.rich()).await?;
    check_history_samples(client, schema, evidence.rich()).await
}

async fn execute_exact_point_query(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    query: ExpectedPointQuery,
) -> Result<(), TpccError> {
    let scope = query.scope;
    let rendered = render_and_only_point_sql(schema, scope, &query.logical_sql)?;
    let state = ExactRowsFold::new(&query);
    let response = client
        .exec_stream_fold(
            &rendered,
            state,
            |columns, state| state.accept_meta(columns),
            |_, row, state| state.accept_row(row),
        )
        .await
        .map_err(|error| sanitize_exec_error(scope, error))?;
    finish_exact_response(scope, response)
}

fn finish_exact_response(
    scope: QueryScope,
    response: FoldStreamResponse<ExactRowsFold>,
) -> Result<(), TpccError> {
    match response {
        FoldStreamResponse::Query {
            row_count, state, ..
        } => state
            .finish(row_count)
            .map_err(|_| TpccError::Protocol(scope.message("result mismatch"))),
        FoldStreamResponse::CommandOk => Err(TpccError::Protocol(
            scope.message("returned a non-query terminal"),
        )),
        FoldStreamResponse::TransactionAbort { diagnostic: _ } => {
            Err(TpccError::Abort(scope.message("aborted")))
        }
        FoldStreamResponse::Error { diagnostic: _ } => {
            Err(TpccError::QueryError(scope.message("failed")))
        }
    }
}

fn render_and_only_point_sql(
    schema: &RuntimeSchema,
    scope: QueryScope,
    logical_sql: &str,
) -> Result<String, TpccError> {
    let tokens = logical_sql
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let valid_shape = tokens.first().is_some_and(|token| token == "SELECT")
        && tokens.iter().any(|token| token == "WHERE")
        && logical_sql.contains('=')
        && !tokens
            .iter()
            .any(|token| matches!(token.as_str(), "OR" | "UNION" | "LIMIT"));
    if !valid_shape || logical_sql.as_bytes().contains(&0) {
        return Err(TpccError::Protocol(
            scope.message("has invalid point-query shape"),
        ));
    }
    Ok(terminate_sql(&schema.render_sql(logical_sql)))
}

fn sanitize_exec_error(scope: QueryScope, error: TpccError) -> TpccError {
    match error {
        TpccError::Connection(_) | TpccError::Io(_) => {
            TpccError::Connection(scope.message("transport failed"))
        }
        TpccError::Abort(_) => TpccError::Abort(scope.message("aborted")),
        TpccError::QueryError(_) => TpccError::QueryError(scope.message("failed")),
        TpccError::ParseError(_) | TpccError::Protocol(_) => {
            TpccError::Protocol(scope.message("protocol failed"))
        }
        TpccError::Timeout { .. } => TpccError::Timeout {
            context: scope.message("response deadline expired"),
        },
    }
}

fn wire_value_has_type(value: &WireValue, sql_type: SqlType) -> bool {
    matches!(
        (value, sql_type),
        (WireValue::Int32(_), SqlType::Int32)
            | (WireValue::Float32(_), SqlType::Float32)
            | (WireValue::Char(_), SqlType::Char)
    )
}

fn terminate_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.ends_with(';') {
        trimmed.to_owned()
    } else {
        format!("{trimmed};")
    }
}

fn require_nonempty_samples(
    domain: SampleDomain,
    observed_count: u64,
    sample_count: usize,
) -> Result<(), TpccError> {
    if observed_count == 0 || sample_count == 0 {
        Err(TpccError::Protocol(format!(
            "recovery {} sample evidence is empty",
            domain.label()
        )))
    } else {
        Ok(())
    }
}

fn checked_query_ordinal(
    domain: SampleDomain,
    sample_index: usize,
    queries_per_sample: usize,
) -> Result<usize, TpccError> {
    sample_index
        .checked_mul(queries_per_sample)
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| {
            TpccError::Protocol(format!(
                "recovery {} sample query ordinal overflow",
                domain.label()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_schema::{LogicalTable, SchemaMode};

    fn columns(types: &[SqlType]) -> Vec<Column> {
        types
            .iter()
            .enumerate()
            .map(|(index, sql_type)| Column {
                name: format!("ignored_{index}"),
                sql_type: *sql_type,
            })
            .collect()
    }

    fn point_query(rows: Vec<Vec<WireValue>>) -> ExpectedPointQuery {
        ExpectedPointQuery::new(
            QueryScope::new(SampleDomain::Stock, 3),
            "SELECT stock.s_w_id, stock.s_i_id FROM stock WHERE stock.s_w_id = 1 AND stock.s_i_id = 2"
                .to_owned(),
            vec![SqlType::Int32, SqlType::Int32],
            rows,
        )
        .unwrap()
    }

    fn delivered_new_order_expected() -> NewOrderExpected {
        NewOrderExpected {
            warehouse_id: 2,
            district_id: 3,
            order_id: 4_001,
            customer_id: 37,
            entry_timestamp: b"2026-07-29 12:34:56".to_vec(),
            carrier_id: 7,
            line_count: 2,
            all_local: 0,
            queue_present: false,
            lines: vec![
                NewOrderLineExpected {
                    number: 1,
                    item_id: 91,
                    supply_warehouse: 2,
                    delivery_timestamp: b"2026-07-29 12:35:00".to_vec(),
                    quantity: 5,
                    amount_bits: (-0.0_f32).to_bits(),
                    district_info: b"ABCDEFGHIJKLMNOPQRSTUVWX".to_vec(),
                },
                NewOrderLineExpected {
                    number: 2,
                    item_id: 92,
                    supply_warehouse: 1,
                    delivery_timestamp: b"2026-07-29 12:35:00".to_vec(),
                    quantity: 6,
                    amount_bits: 0x3f80_0001,
                    district_info: b"abcdefghijklmnopqrstuvwx".to_vec(),
                },
            ],
        }
    }

    fn delivery_expected() -> DeliveryExpected {
        DeliveryExpected {
            warehouse_id: 4,
            district_id: 8,
            order_id: 3_777,
            customer_id: 812,
            carrier_id: 9,
            queue_present: false,
            delivery_timestamp: b"2026-07-29 13:14:15".to_vec(),
            lines: vec![
                DeliveryLineExpected {
                    number: 1,
                    delivery_timestamp: b"2026-07-29 13:14:15".to_vec(),
                    amount_bits: 0x4120_0001,
                },
                DeliveryLineExpected {
                    number: 2,
                    delivery_timestamp: b"2026-07-29 13:14:15".to_vec(),
                    amount_bits: (-0.0_f32).to_bits(),
                },
            ],
        }
    }

    fn stock_expected() -> StockExpected {
        StockExpected {
            warehouse_id: 6,
            item_id: 9_001,
            quantity: 73,
            ytd_bits: 0x8000_0000,
            order_count: 41,
            remote_count: 3,
        }
    }

    fn customer_expected() -> CustomerExpected {
        CustomerExpected {
            warehouse_id: 2,
            district_id: 7,
            customer_id: 811,
            credit: b"GC".to_vec(),
            balance_bits: 0xc120_0001,
            ytd_payment_bits: 0x4120_0001,
            payment_count: 9,
            delivery_count: 17,
        }
    }

    fn bad_credit_expected() -> BadCreditExpected {
        BadCreditExpected {
            warehouse_id: 3,
            district_id: 4,
            customer_id: 512,
            credit: b"BC".to_vec(),
            payment_count: 6,
            data: b"512 4 3 4 3 1.00 | setup-data".to_vec(),
        }
    }

    fn history_key() -> HistoryPointKey {
        HistoryPointKey {
            customer_id: 713,
            customer_district_id: 6,
            customer_warehouse_id: 4,
            home_district_id: 8,
            home_warehouse_id: 2,
        }
    }

    fn history_expected() -> HistoryGroupExpected {
        HistoryGroupExpected {
            key: history_key(),
            tuples: vec![
                HistoryTupleExpected {
                    timestamp: b"2026-07-29 14:15:16".to_vec(),
                    amount_bits: 0x3f80_0001,
                    data: b"setup-collision".to_vec(),
                    multiplicity: 2,
                },
                HistoryTupleExpected {
                    timestamp: b"2026-07-29 14:15:17".to_vec(),
                    amount_bits: (-0.0_f32).to_bits(),
                    data: b"runtime-only".to_vec(),
                    multiplicity: 1,
                },
            ],
        }
    }

    fn history_row(
        key: HistoryPointKey,
        timestamp: &[u8],
        amount_bits: u32,
        data: &[u8],
    ) -> Vec<WireValue> {
        vec![
            WireValue::Int32(key.customer_id),
            WireValue::Int32(key.customer_district_id),
            WireValue::Int32(key.customer_warehouse_id),
            WireValue::Int32(key.home_district_id),
            WireValue::Int32(key.home_warehouse_id),
            WireValue::Char(timestamp.to_vec()),
            WireValue::Float32(amount_bits),
            WireValue::Char(data.to_vec()),
        ]
    }

    #[test]
    fn exact_fold_accepts_reordered_rows_and_ignores_column_labels() {
        let query = point_query(vec![
            vec![WireValue::Int32(1), WireValue::Int32(2)],
            vec![WireValue::Int32(3), WireValue::Int32(4)],
        ]);
        let mut state = ExactRowsFold::new(&query);
        state
            .accept_meta(&columns(&[SqlType::Int32, SqlType::Int32]))
            .unwrap();
        state
            .accept_row(vec![WireValue::Int32(3), WireValue::Int32(4)])
            .unwrap();
        state
            .accept_row(vec![WireValue::Int32(1), WireValue::Int32(2)])
            .unwrap();
        state.finish(2).unwrap();
    }

    #[test]
    fn exact_fold_rejects_duplicate_missing_extra_wrong_type_and_null() {
        let query = point_query(vec![vec![WireValue::Int32(1), WireValue::Int32(2)]]);
        for bad_row in [
            vec![WireValue::Int32(1), WireValue::Int32(2)],
            vec![WireValue::Int32(9), WireValue::Int32(2)],
            vec![WireValue::Float32(1), WireValue::Int32(2)],
            vec![WireValue::Null, WireValue::Int32(2)],
        ] {
            let mut state = ExactRowsFold::new(&query);
            state
                .accept_meta(&columns(&[SqlType::Int32, SqlType::Int32]))
                .unwrap();
            state
                .accept_row(vec![WireValue::Int32(1), WireValue::Int32(2)])
                .unwrap();
            assert!(state.accept_row(bad_row).is_err());
        }

        let mut missing = ExactRowsFold::new(&query);
        missing
            .accept_meta(&columns(&[SqlType::Int32, SqlType::Int32]))
            .unwrap();
        assert!(missing.finish(0).is_err());
        assert!(ExpectedPointQuery::new(
            QueryScope::new(SampleDomain::Stock, 4),
            "SELECT stock.s_w_id FROM stock WHERE stock.s_w_id = 1".to_owned(),
            vec![SqlType::Int32],
            vec![vec![WireValue::Null]],
        )
        .is_err());
    }

    #[test]
    fn metadata_requires_exact_column_types_but_not_names() {
        let query = point_query(Vec::new());
        let mut wrong_count = ExactRowsFold::new(&query);
        assert!(wrong_count
            .accept_meta(&columns(&[SqlType::Int32]))
            .is_err());

        let mut wrong_type = ExactRowsFold::new(&query);
        assert!(wrong_type
            .accept_meta(&columns(&[SqlType::Int32, SqlType::Float32]))
            .is_err());
    }

    #[test]
    fn point_renderer_is_opaque_terminated_and_forbids_wide_shapes() {
        let schema = RuntimeSchema::opaque(0x1234).unwrap();
        assert_eq!(schema.mode(), SchemaMode::LocalSeedOpaqueV1);
        let query = point_query(Vec::new());
        let rendered = render_and_only_point_sql(&schema, query.scope, &query.logical_sql).unwrap();
        assert!(rendered.ends_with(';'));
        for logical in [LogicalTable::Stock.canonical(), "s_w_id", "s_i_id"] {
            assert!(
                !rendered
                    .split(|character: char| {
                        !(character.is_ascii_alphanumeric() || character == '_')
                    })
                    .any(|token| token == logical),
                "logical identifier leaked: {logical}"
            );
        }
        for sql in [
            "SELECT a FROM t WHERE a = 1 OR b = 2",
            "SELECT a FROM t WHERE a = 1 LIMIT 1",
            "SELECT a FROM t UNION SELECT a FROM u WHERE a = 1",
        ] {
            assert!(render_and_only_point_sql(&schema, query.scope, sql).is_err());
        }
    }

    #[test]
    fn all_error_paths_strip_sql_keys_answers_and_server_diagnostics() {
        let scope = QueryScope::new(SampleDomain::Customer, 17);
        let secret = "secret_table key=417 expected=9 actual=10";
        let errors = [
            sanitize_exec_error(scope, TpccError::Connection(secret.to_owned())),
            sanitize_exec_error(scope, TpccError::Abort(secret.to_owned())),
            sanitize_exec_error(scope, TpccError::QueryError(secret.to_owned())),
            sanitize_exec_error(scope, TpccError::ParseError(secret.to_owned())),
            sanitize_exec_error(scope, TpccError::Protocol(secret.to_owned())),
            sanitize_exec_error(
                scope,
                TpccError::Timeout {
                    context: secret.to_owned(),
                },
            ),
            finish_exact_response(
                scope,
                FoldStreamResponse::TransactionAbort {
                    diagnostic: secret.to_owned(),
                },
            )
            .unwrap_err(),
            finish_exact_response(
                scope,
                FoldStreamResponse::Error {
                    diagnostic: secret.to_owned(),
                },
            )
            .unwrap_err(),
        ];
        for error in errors {
            let rendered = error.to_string();
            assert!(!rendered.contains(secret), "{rendered}");
            assert!(!rendered.contains("417"), "{rendered}");
            assert!(rendered.contains("customer"), "{rendered}");
            assert!(rendered.contains("17"), "{rendered}");
        }
    }

    #[test]
    fn every_domain_has_a_fixed_nonsecret_label() {
        assert_eq!(SampleDomain::NewOrder.label(), "new-order");
        assert_eq!(SampleDomain::Delivery.label(), "delivery");
        assert_eq!(SampleDomain::Stock.label(), "stock");
        assert_eq!(SampleDomain::Customer.label(), "customer");
        assert_eq!(SampleDomain::BadCredit.label(), "bad-credit");
        assert_eq!(SampleDomain::History.label(), "history");
    }

    #[test]
    fn new_order_queries_cover_full_rows_and_later_delivery_overlay() {
        let expected = delivered_new_order_expected();
        let queries = new_order_point_queries(&expected, 10).unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(
            queries[0].scope,
            QueryScope::new(SampleDomain::NewOrder, 10)
        );
        assert_eq!(
            queries[1].scope,
            QueryScope::new(SampleDomain::NewOrder, 11)
        );
        assert_eq!(
            queries[2].scope,
            QueryScope::new(SampleDomain::NewOrder, 12)
        );
        assert_eq!(
            queries[0].expected_rows,
            vec![vec![
                WireValue::Int32(4_001),
                WireValue::Int32(3),
                WireValue::Int32(2),
                WireValue::Int32(37),
                WireValue::Char(b"2026-07-29 12:34:56".to_vec()),
                WireValue::Int32(7),
                WireValue::Int32(2),
                WireValue::Int32(0),
            ]]
        );
        assert!(queries[1].expected_rows.is_empty());
        assert_eq!(queries[2].expected_rows.len(), 2);
        assert_eq!(
            queries[2].expected_rows[0][8],
            WireValue::Float32((-0.0_f32).to_bits())
        );
        assert_eq!(
            queries[2].expected_rows[1][8],
            WireValue::Float32(0x3f80_0001)
        );
        assert_eq!(
            queries[2].expected_rows[0][6],
            WireValue::Char(b"2026-07-29 12:35:00".to_vec())
        );
    }

    #[test]
    fn new_order_queries_are_opaque_and_and_only_points() {
        let schema = RuntimeSchema::opaque(0x5678).unwrap();
        let queries = new_order_point_queries(&delivered_new_order_expected(), 1).unwrap();
        for query in queries {
            let tokens = query
                .logical_sql
                .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .filter(|token| !token.is_empty())
                .map(str::to_ascii_uppercase)
                .collect::<Vec<_>>();
            assert!(tokens.iter().any(|token| token == "WHERE"));
            assert!(!tokens
                .iter()
                .any(|token| matches!(token.as_str(), "OR" | "UNION" | "LIMIT")));

            let rendered =
                render_and_only_point_sql(&schema, query.scope, &query.logical_sql).unwrap();
            for logical in [
                "orders",
                "new_orders",
                "order_line",
                "o_id",
                "no_o_id",
                "ol_o_id",
            ] {
                assert!(!rendered
                    .split(|character: char| {
                        !(character.is_ascii_alphanumeric() || character == '_')
                    })
                    .any(|token| token == logical));
            }
        }
    }

    #[test]
    fn new_order_nonempty_gate_fails_closed() {
        assert!(require_nonempty_samples(SampleDomain::NewOrder, 0, 0).is_err());
        assert!(require_nonempty_samples(SampleDomain::NewOrder, 1, 0).is_err());
        assert!(require_nonempty_samples(SampleDomain::NewOrder, 1, 1).is_ok());
        assert_eq!(
            checked_query_ordinal(SampleDomain::NewOrder, 2, 3).unwrap(),
            7
        );
    }

    #[test]
    fn delivery_queries_require_final_order_queue_and_line_state() {
        let expected = delivery_expected();
        let queries = delivery_point_queries(&expected, 20).unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(queries[0].column_types, vec![SqlType::Int32; 6]);
        assert_eq!(
            queries[0].expected_rows,
            vec![vec![
                WireValue::Int32(3_777),
                WireValue::Int32(8),
                WireValue::Int32(4),
                WireValue::Int32(812),
                WireValue::Int32(9),
                WireValue::Int32(2),
            ]]
        );
        assert!(queries[1].expected_rows.is_empty());
        assert_eq!(queries[2].expected_rows.len(), 2);
        assert_eq!(
            queries[2].expected_rows[0][4],
            WireValue::Char(b"2026-07-29 13:14:15".to_vec())
        );
        assert_eq!(
            queries[2].expected_rows[0][5],
            WireValue::Float32(0x4120_0001)
        );
        assert_eq!(
            queries[2].expected_rows[1][5],
            WireValue::Float32((-0.0_f32).to_bits())
        );
    }

    #[test]
    fn delivery_query_builder_rejects_queue_or_timestamp_regression() {
        let mut queued = delivery_expected();
        queued.queue_present = true;
        assert!(delivery_point_queries(&queued, 1).is_err());

        let mut stale_line = delivery_expected();
        stale_line.lines[0].delivery_timestamp = b"2026-07-29 00:00:00".to_vec();
        assert!(delivery_point_queries(&stale_line, 1).is_err());
    }

    #[test]
    fn delivery_queries_are_opaque_and_and_only_points() {
        let schema = RuntimeSchema::opaque(0xabcd).unwrap();
        let queries = delivery_point_queries(&delivery_expected(), 1).unwrap();
        for query in queries {
            let rendered =
                render_and_only_point_sql(&schema, query.scope, &query.logical_sql).unwrap();
            for logical in [
                "orders",
                "new_orders",
                "order_line",
                "o_carrier_id",
                "no_o_id",
                "ol_delivery_d",
            ] {
                assert!(!rendered
                    .split(|character: char| {
                        !(character.is_ascii_alphanumeric() || character == '_')
                    })
                    .any(|token| token == logical));
            }
        }
        assert!(require_nonempty_samples(SampleDomain::Delivery, 0, 0).is_err());
        assert!(require_nonempty_samples(SampleDomain::Delivery, 1, 1).is_ok());
    }

    #[test]
    fn stock_query_keeps_all_endpoint_fields_bit_exact() {
        let query = stock_point_query(&stock_expected(), 5).unwrap();
        assert_eq!(
            query.column_types,
            vec![
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Float32,
                SqlType::Int32,
                SqlType::Int32,
            ]
        );
        assert_eq!(
            query.expected_rows,
            vec![vec![
                WireValue::Int32(6),
                WireValue::Int32(9_001),
                WireValue::Int32(73),
                WireValue::Float32(0x8000_0000),
                WireValue::Int32(41),
                WireValue::Int32(3),
            ]]
        );
    }

    #[test]
    fn normal_customer_query_includes_credit_and_four_numeric_endpoints() {
        let query = customer_point_query(&customer_expected(), 6).unwrap();
        assert_eq!(
            query.expected_rows,
            vec![vec![
                WireValue::Int32(2),
                WireValue::Int32(7),
                WireValue::Int32(811),
                WireValue::Char(b"GC".to_vec()),
                WireValue::Float32(0xc120_0001),
                WireValue::Float32(0x4120_0001),
                WireValue::Int32(9),
                WireValue::Int32(17),
            ]]
        );
        let generator = TpccDataGen::with_seed(2, 0x1234);
        let profile = generator.initial_customer_profile(2, 7, 811).unwrap();
        assert!(matches!(profile.credit().as_slice(), b"GC" | b"BC"));
    }

    #[test]
    fn bad_credit_query_is_independent_of_later_delivery_state() {
        let query = bad_credit_point_query(&bad_credit_expected(), 7).unwrap();
        assert_eq!(
            query.expected_rows,
            vec![vec![
                WireValue::Int32(3),
                WireValue::Int32(4),
                WireValue::Int32(512),
                WireValue::Char(b"BC".to_vec()),
                WireValue::Int32(6),
                WireValue::Char(b"512 4 3 4 3 1.00 | setup-data".to_vec()),
            ]]
        );
        let tokens = query
            .logical_sql
            .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
            .collect::<Vec<_>>();
        for forbidden in [
            "c_balance",
            "c_ytd_payment",
            "c_delivery_cnt",
            "c_payment_cnt",
        ] {
            assert_eq!(
                tokens.iter().filter(|token| **token == forbidden).count(),
                usize::from(forbidden == "c_payment_cnt")
            );
        }
    }

    #[test]
    fn numeric_customer_domains_render_as_opaque_and_only_points() {
        let schema = RuntimeSchema::opaque(0x9988).unwrap();
        let queries = [
            stock_point_query(&stock_expected(), 1).unwrap(),
            customer_point_query(&customer_expected(), 1).unwrap(),
            bad_credit_point_query(&bad_credit_expected(), 1).unwrap(),
        ];
        for query in queries {
            let rendered =
                render_and_only_point_sql(&schema, query.scope, &query.logical_sql).unwrap();
            for logical in ["stock", "customer", "s_ytd", "c_balance", "c_data"] {
                assert!(!rendered
                    .split(|character: char| {
                        !(character.is_ascii_alphanumeric() || character == '_')
                    })
                    .any(|token| token == logical));
            }
        }
        assert!(require_nonempty_samples(SampleDomain::Stock, 1, 1).is_ok());
        assert!(require_nonempty_samples(SampleDomain::Customer, 1, 1).is_ok());
        assert!(require_nonempty_samples(SampleDomain::BadCredit, 1, 1).is_ok());
        assert!(require_nonempty_samples(SampleDomain::Stock, 1, 0).is_err());
        assert!(require_nonempty_samples(SampleDomain::Customer, 0, 1).is_err());
        assert!(require_nonempty_samples(SampleDomain::BadCredit, 0, 0).is_err());
    }

    #[test]
    fn history_fold_counts_retained_tuples_and_ignores_other_group_rows() {
        let key = history_key();
        let mut state = HistoryGroupFold::new(history_expected());
        state
            .accept_meta(&columns(&[
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Char,
                SqlType::Float32,
                SqlType::Char,
            ]))
            .unwrap();
        state
            .accept_row(history_row(
                key,
                b"2026-07-29 14:15:18",
                0x4120_0000,
                b"unretained-but-same-group",
            ))
            .unwrap();
        state
            .accept_row(history_row(
                key,
                b"2026-07-29 14:15:16",
                0x3f80_0001,
                b"setup-collision",
            ))
            .unwrap();
        state
            .accept_row(history_row(
                key,
                b"2026-07-29 14:15:17",
                (-0.0_f32).to_bits(),
                b"runtime-only",
            ))
            .unwrap();
        state
            .accept_row(history_row(
                key,
                b"2026-07-29 14:15:16",
                0x3f80_0001,
                b"setup-collision",
            ))
            .unwrap();
        assert_eq!(state.retained.len(), 2);
        state.finish().unwrap();
    }

    #[test]
    fn history_fold_rejects_missing_extra_identical_wrong_key_type_and_null() {
        let key = history_key();
        let mut missing = HistoryGroupFold::new(history_expected());
        missing
            .accept_meta(&columns(&[
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Int32,
                SqlType::Char,
                SqlType::Float32,
                SqlType::Char,
            ]))
            .unwrap();
        assert!(missing.finish().is_err());

        let one_tuple = || HistoryGroupExpected {
            key,
            tuples: vec![HistoryTupleExpected {
                timestamp: b"2026-07-29 14:15:16".to_vec(),
                amount_bits: 0x3f80_0001,
                data: b"selected".to_vec(),
                multiplicity: 1,
            }],
        };
        let expected_row = history_row(key, b"2026-07-29 14:15:16", 0x3f80_0001, b"selected");
        let expected_types = [
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Char,
            SqlType::Float32,
            SqlType::Char,
        ];

        let mut extra = HistoryGroupFold::new(one_tuple());
        extra.accept_meta(&columns(&expected_types)).unwrap();
        extra.accept_row(expected_row.clone()).unwrap();
        assert!(extra.accept_row(expected_row.clone()).is_err());

        let mut wrong_key = HistoryGroupFold::new(one_tuple());
        wrong_key.accept_meta(&columns(&expected_types)).unwrap();
        let mut wrong_key_row = expected_row.clone();
        wrong_key_row[0] = WireValue::Int32(714);
        assert!(wrong_key.accept_row(wrong_key_row).is_err());

        let mut wrong_type = HistoryGroupFold::new(one_tuple());
        wrong_type.accept_meta(&columns(&expected_types)).unwrap();
        let mut wrong_type_row = expected_row.clone();
        wrong_type_row[6] = WireValue::Int32(1);
        assert!(wrong_type.accept_row(wrong_type_row).is_err());

        let mut null = HistoryGroupFold::new(one_tuple());
        null.accept_meta(&columns(&expected_types)).unwrap();
        let mut null_row = expected_row;
        null_row[7] = WireValue::Null;
        assert!(null.accept_row(null_row).is_err());
    }

    #[test]
    fn history_query_selects_all_columns_with_one_five_key_and_group() {
        let logical = history_group_sql(history_key());
        for column in [
            "h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data",
        ] {
            assert!(logical.contains(column), "{column}");
        }
        let tokens = logical
            .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
            .filter(|token| !token.is_empty())
            .map(str::to_ascii_uppercase)
            .collect::<Vec<_>>();
        assert_eq!(tokens.iter().filter(|token| *token == "WHERE").count(), 1);
        assert_eq!(tokens.iter().filter(|token| *token == "AND").count(), 4);
        assert!(!tokens
            .iter()
            .any(|token| matches!(token.as_str(), "OR" | "UNION" | "LIMIT")));

        let schema = RuntimeSchema::opaque(0x8877).unwrap();
        let rendered =
            render_and_only_point_sql(&schema, QueryScope::new(SampleDomain::History, 1), &logical)
                .unwrap();
        for identifier in ["history", "h_c_id", "h_date", "h_amount", "h_data"] {
            assert!(!rendered
                .split(|character: char| {
                    !(character.is_ascii_alphanumeric() || character == '_')
                })
                .any(|token| token == identifier));
        }
        assert_eq!(
            checked_query_ordinal(SampleDomain::History, 1, 1).unwrap(),
            2
        );
    }

    #[test]
    fn history_terminal_errors_are_static_and_entry_uses_terminal_evidence_view() {
        let scope = QueryScope::new(SampleDomain::History, 9);
        let secret = "history key=713 expected=2 actual=3 runtime_table";
        for error in [
            finish_history_response(
                scope,
                FoldStreamResponse::TransactionAbort {
                    diagnostic: secret.to_owned(),
                },
            )
            .unwrap_err(),
            finish_history_response(
                scope,
                FoldStreamResponse::Error {
                    diagnostic: secret.to_owned(),
                },
            )
            .unwrap_err(),
        ] {
            let rendered = error.to_string();
            assert!(rendered.contains("history"));
            assert!(rendered.contains('9'));
            assert!(!rendered.contains(secret));
            assert!(!rendered.contains("713"));
        }
        let _entry = check_recovery_samples;
        assert!(require_nonempty_samples(SampleDomain::History, 1, 1).is_ok());
        assert!(require_nonempty_samples(SampleDomain::History, 1, 0).is_err());
    }
}
