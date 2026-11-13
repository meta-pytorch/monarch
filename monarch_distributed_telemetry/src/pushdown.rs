/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Pushes partial aggregation down to telemetry collectors.
//!
//! [`PushDownCollectorAggregates`] finds an aggregate over a subplan whose
//! rows each come from one collector, runs that subplan and a partial
//! aggregate on every collector, and finishes the aggregate at the root.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Column;
use datafusion::common::NullEquality;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::TableProvider;
use datafusion::datasource::provider_as_source;
use datafusion::datasource::source_as_provider;
use datafusion::error::Result as DFResult;
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::functions_aggregate::expr_fn::min;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::Aggregate;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::JoinType;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::Projection;
use datafusion::logical_expr::cast;
use datafusion::logical_expr::col;
use datafusion::logical_expr::lit;
use datafusion::optimizer::ApplyOrder;
use datafusion::optimizer::OptimizerConfig;
use datafusion::optimizer::OptimizerRule;
use datafusion::sql::unparser::plan_to_sql;

/// Columns whose equal values are recorded on the same host: a message and
/// its status events are both recorded by the receiving proc.
const MESSAGE_KEYS: &[(&str, &str)] =
    &[("messages", "id"), ("message_status_events", "message_id")];

/// Prefix of the table names that pushed subplans are scanned as.
const PUSHED_TABLE_PREFIX: &str = "__collector_sql";

/// The tables a query engine scans from collectors.
pub(crate) trait CollectorTables: Send + Sync + std::fmt::Debug {
    /// Whether `provider` scans a table whose rows are split across collectors.
    fn is_collector_table(&self, provider: &dyn TableProvider) -> bool;

    /// A table that runs `sql` on every collector and concatenates the
    /// results, which have `schema`.
    fn sql_table(&self, sql: String, schema: SchemaRef) -> Arc<dyn TableProvider>;
}

/// Runs the collector-local part of an aggregate, and a partial aggregate over
/// it, on the collectors.
///
/// The local part is the largest subplan in which every row comes from one
/// collector and that reads a message table: one collector table, or
/// collector tables inner-joined on [`MESSAGE_KEYS`]. Collectors group it by the columns the rest of the plan
/// reads, so the joins above it see one row per group, carrying partial
/// `COUNT`, `SUM`, `MIN`, and `MAX` results that the root combines.
#[derive(Debug)]
pub(crate) struct PushDownCollectorAggregates {
    tables: Arc<dyn CollectorTables>,
}

impl PushDownCollectorAggregates {
    pub(crate) fn new(tables: Arc<dyn CollectorTables>) -> Self {
        Self { tables }
    }
}

impl OptimizerRule for PushDownCollectorAggregates {
    fn name(&self) -> &str {
        "push_down_collector_aggregates"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = &plan else {
            return Ok(Transformed::no(plan));
        };
        // A plan that cannot be pushed, including one the unparser cannot
        // express, runs as before.
        match self.push_down(aggregate, config) {
            Ok(Some(pushed)) => Ok(Transformed::yes(pushed)),
            Ok(None) => Ok(Transformed::no(plan)),
            Err(error) => {
                tracing::debug!("not pushing aggregate to collectors: {error}");
                Ok(Transformed::no(plan))
            }
        }
    }
}

/// An aggregate function that collectors can compute partially.
#[derive(Clone, Copy)]
enum Partial {
    Count,
    Sum,
    Min,
    Max,
}

impl Partial {
    fn of(expr: &Expr) -> Option<(Self, &Expr)> {
        let Expr::AggregateFunction(function) = unalias(expr) else {
            return None;
        };
        let params = &function.params;
        if params.distinct
            || params.filter.is_some()
            || !params.order_by.is_empty()
            || params.null_treatment.is_some()
            || params.args.len() != 1
        {
            return None;
        }
        let partial = match function.func.name() {
            "count" => Self::Count,
            "sum" => Self::Sum,
            "min" => Self::Min,
            "max" => Self::Max,
            _ => return None,
        };
        Some((partial, &params.args[0]))
    }

    fn partial(self, arg: Expr) -> Expr {
        match self {
            Self::Count => count(arg),
            Self::Sum => sum(arg),
            Self::Min => min(arg),
            Self::Max => max(arg),
        }
    }

    fn combine(self, partial: Expr) -> Expr {
        match self {
            Self::Count | Self::Sum => sum(partial),
            Self::Min => min(partial),
            Self::Max => max(partial),
        }
    }
}

fn unalias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => unalias(&alias.expr),
        expr => expr,
    }
}

fn is_plain_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_))
}

fn contains_pushed_table(plan: &LogicalPlan) -> bool {
    plan.exists(|node| {
        Ok(matches!(node, LogicalPlan::TableScan(scan)
            if scan.table_name.table().starts_with(PUSHED_TABLE_PREFIX)))
    })
    .unwrap_or(true)
}

fn has_subquery(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(matches!(
            e,
            Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::OuterReferenceColumn(..)
        ))
    })
    .unwrap_or(true)
}

/// Whether `expr` can run on a collector with the same result.
fn is_portable(expr: &Expr) -> bool {
    !expr.is_volatile() && !has_subquery(expr)
}

/// For each output column of a collector-local subplan, the collector table
/// column it passes through unchanged, if any.
type Origins = Vec<Option<(String, String)>>;

/// A collector-local subplan, found by its child indices from the
/// aggregate's input.
struct Local {
    path: Vec<usize>,
    message_tables: usize,
}

impl PushDownCollectorAggregates {
    /// Returns the column origins of `plan` if every row it produces comes
    /// from one collector.
    fn local_origins(&self, plan: &LogicalPlan) -> Option<Origins> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                if scan.fetch.is_some() || !scan.filters.iter().all(is_portable) {
                    return None;
                }
                let provider = source_as_provider(&scan.source).ok()?;
                if !self.tables.is_collector_table(provider.as_ref()) {
                    return None;
                }
                let table = scan.table_name.table().to_string();
                Some(
                    scan.projected_schema
                        .fields()
                        .iter()
                        .map(|field| Some((table.clone(), field.name().clone())))
                        .collect(),
                )
            }
            LogicalPlan::SubqueryAlias(alias) => self.local_origins(&alias.input),
            LogicalPlan::Filter(filter) if is_portable(&filter.predicate) => {
                self.local_origins(&filter.input)
            }
            LogicalPlan::Projection(projection) if projection.expr.iter().all(is_portable) => {
                let input = self.local_origins(&projection.input)?;
                let schema = projection.input.schema();
                Some(
                    projection
                        .expr
                        .iter()
                        .map(|expr| match unalias(expr) {
                            Expr::Column(column) => schema
                                .index_of_column(column)
                                .ok()
                                .and_then(|index| input[index].clone()),
                            _ => None,
                        })
                        .collect(),
                )
            }
            LogicalPlan::Join(join)
                if join.join_type == JoinType::Inner
                    && join.null_equality == NullEquality::NullEqualsNothing
                    && join.filter.as_ref().is_none_or(is_portable) =>
            {
                let left = self.local_origins(&join.left)?;
                let right = self.local_origins(&join.right)?;
                let origin = |plan: &LogicalPlan, origins: &Origins, expr: &Expr| match expr {
                    Expr::Column(column) => plan
                        .schema()
                        .index_of_column(column)
                        .ok()
                        .and_then(|index| origins[index].clone()),
                    _ => None,
                };
                let is_message_key = |origin: Option<(String, String)>| {
                    origin.is_some_and(|(table, column)| {
                        MESSAGE_KEYS.contains(&(table.as_str(), column.as_str()))
                    })
                };
                let colocated = join.on.iter().any(|(l, r)| {
                    is_message_key(origin(&join.left, &left, l))
                        && is_message_key(origin(&join.right, &right, r))
                });
                colocated.then(|| left.into_iter().chain(right).collect())
            }
            _ => None,
        }
    }

    /// Finds the collector-local subplans under `plan` that the operators
    /// above them let partial aggregation pass.
    fn find_local(&self, plan: &LogicalPlan, path: &mut Vec<usize>, found: &mut Vec<Local>) {
        if self.local_origins(plan).is_some() {
            let mut message_tables = 0;
            plan.apply(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    let table = scan.table_name.table();
                    if MESSAGE_KEYS.iter().any(|(t, _)| *t == table) {
                        message_tables += 1;
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("counting tables cannot fail");
            found.push(Local {
                path: path.clone(),
                message_tables,
            });
            return;
        }
        let passes = match plan {
            LogicalPlan::Projection(projection) => projection.expr.iter().all(is_plain_column),
            LogicalPlan::Filter(filter) => is_portable(&filter.predicate),
            LogicalPlan::Join(join) => join.join_type == JoinType::Inner,
            _ => false,
        };
        if !passes {
            return;
        }
        for (index, input) in plan.inputs().into_iter().enumerate() {
            path.push(index);
            self.find_local(input, path, found);
            path.pop();
        }
    }

    fn push_down(
        &self,
        aggregate: &Aggregate,
        config: &dyn OptimizerConfig,
    ) -> DFResult<Option<LogicalPlan>> {
        if aggregate
            .group_expr
            .iter()
            .any(|e| matches!(e, Expr::GroupingSet(_)))
        {
            return Ok(None);
        }
        let Some(partials) = aggregate
            .aggr_expr
            .iter()
            .map(Partial::of)
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };

        if contains_pushed_table(&aggregate.input) {
            return Ok(None);
        }
        let mut found = Vec::new();
        self.find_local(&aggregate.input, &mut Vec::new(), &mut found);
        // Only message tables are large enough to repay a collector query.
        // Prefer the subplan with the most of them, then the leftmost.
        found.retain(|local| local.message_tables > 0);
        found.sort_by_key(|local| std::cmp::Reverse(local.message_tables));
        for local in found {
            if let Some(plan) = self.push_down_to(aggregate, &partials, &local, config)? {
                return Ok(Some(plan));
            }
        }
        Ok(None)
    }

    fn push_down_to(
        &self,
        aggregate: &Aggregate,
        partials: &[(Partial, &Expr)],
        local: &Local,
        config: &dyn OptimizerConfig,
    ) -> DFResult<Option<LogicalPlan>> {
        let mut nodes = vec![aggregate.input.as_ref()];
        for &index in &local.path {
            let node = nodes.last().unwrap().inputs()[index];
            nodes.push(node);
        }
        let local_plan = *nodes.last().unwrap();
        let above = &nodes[..nodes.len() - 1];
        let local_columns: HashSet<Column> = local_plan.schema().columns().into_iter().collect();

        // Aggregate arguments must come from the local subplan, except a
        // count of a constant, which counts the rows the joins produce.
        for (partial, arg) in partials {
            let columns = arg.column_refs();
            let is_count_of_constant = matches!(partial, Partial::Count)
                && matches!(arg, Expr::Literal(value, _) if !value.is_null());
            if columns.is_empty() && !is_count_of_constant {
                return Ok(None);
            }
            if !columns.iter().all(|c| local_columns.contains(*c)) {
                return Ok(None);
            }
        }

        // Group collectors' rows by every local column the plan above reads.
        let mut read_above: HashSet<Column> = HashSet::new();
        for node in above {
            if matches!(node, LogicalPlan::Projection(_)) {
                continue;
            }
            for expr in node.expressions() {
                read_above.extend(expr.column_refs().into_iter().cloned());
            }
        }
        for expr in &aggregate.group_expr {
            read_above.extend(expr.column_refs().into_iter().cloned());
        }
        let group: Vec<Column> = local_plan
            .schema()
            .columns()
            .into_iter()
            .filter(|c| read_above.contains(c))
            .collect();

        let partial_names: Vec<String> = (0..partials.len()).map(|i| format!("__p{i}")).collect();
        let pushed = LogicalPlanBuilder::from(local_plan.clone())
            .aggregate(
                group.iter().cloned().map(Expr::Column),
                partials
                    .iter()
                    .zip(&partial_names)
                    .map(|((partial, arg), name)| partial.partial((*arg).clone()).alias(name)),
            )?
            .project(
                group
                    .iter()
                    .enumerate()
                    .map(|(i, c)| Expr::Column(c.clone()).alias(format!("__g{i}")))
                    .chain(partial_names.iter().map(col)),
            )?
            .build()?;
        let sql = plan_to_sql(&pushed)?.to_string();
        let schema = Arc::new(pushed.schema().as_arrow().clone());
        let table_name = config.alias_generator().next(PUSHED_TABLE_PREFIX);
        let scan = LogicalPlanBuilder::scan(
            table_name.as_str(),
            provider_as_source(self.tables.sql_table(sql, schema)),
            None,
        )?
        .build()?;
        // Restore the local columns' names so the plan above reads them as before.
        let replacement = LogicalPlanBuilder::from(scan)
            .project(
                group
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        col(format!("__g{i}")).alias_qualified(c.relation.clone(), &c.name)
                    })
                    .chain(partial_names.iter().map(col)),
            )?
            .build()?;

        let dropped: HashSet<Column> = local_columns
            .into_iter()
            .filter(|c| !group.contains(c))
            .collect();
        let input = rebuild(above, &local.path, replacement, &dropped, &partial_names)?;

        let combined_names: Vec<String> = (0..partials.len()).map(|i| format!("__c{i}")).collect();
        let combined = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::new(input),
            aggregate.group_expr.clone(),
            partials
                .iter()
                .zip(&partial_names)
                .zip(&combined_names)
                .map(|(((partial, _), partial_name), name)| {
                    partial.combine(col(partial_name)).alias(name)
                })
                .collect(),
        )?);

        // Present the original aggregate's schema.
        let groups = aggregate.group_expr.len();
        let output = aggregate
            .schema
            .iter()
            .enumerate()
            .map(|(i, (qualifier, field))| {
                let expr = if i < groups {
                    Expr::Column(Column::from(combined.schema().qualified_field(i)))
                } else {
                    let combined = col(&combined_names[i - groups]);
                    let combined = match partials[i - groups].0 {
                        // A count over no rows is 0, not the NULL sum of no partials.
                        Partial::Count => coalesce(vec![combined, lit(0i64)]),
                        _ => combined,
                    };
                    cast(combined, field.data_type().clone())
                };
                expr.alias_qualified(qualifier.cloned(), field.name())
            })
            .collect::<Vec<_>>();
        Ok(Some(LogicalPlan::Projection(Projection::try_new(
            output,
            Arc::new(combined),
        )?)))
    }
}

/// Rebuilds the operators `above` the local subplan along `path` over
/// `replacement`, which drops the local columns in `dropped` and adds the
/// partial aggregate columns `partials`.
fn rebuild(
    above: &[&LogicalPlan],
    path: &[usize],
    replacement: LogicalPlan,
    dropped: &HashSet<Column>,
    partials: &[String],
) -> DFResult<LogicalPlan> {
    let Some((node, rest)) = above.split_first() else {
        return Ok(replacement);
    };
    let input = rebuild(rest, &path[1..], replacement, dropped, partials)?;
    match node {
        LogicalPlan::Projection(projection) => {
            let expr = projection
                .expr
                .iter()
                .filter(|e| !matches!(e, Expr::Column(c) if dropped.contains(c)))
                .cloned()
                .chain(partials.iter().map(col))
                .collect();
            Ok(LogicalPlan::Projection(Projection::try_new(
                expr,
                Arc::new(input),
            )?))
        }
        node => {
            let mut inputs: Vec<LogicalPlan> = node.inputs().into_iter().cloned().collect();
            inputs[path[0]] = input;
            node.with_new_exprs(node.expressions(), inputs)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use async_trait::async_trait;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::catalog::Session;
    use datafusion::datasource::MemTable;
    use datafusion::datasource::TableType;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::TableProviderFilterPushDown;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    use super::*;

    /// A collector table: the union of every host's rows.
    #[derive(Debug)]
    struct HostTable {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    #[async_trait]
    impl TableProvider for HostTable {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> DFResult<Vec<TableProviderFilterPushDown>> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }
        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            // Apply the filters this table claims, as collectors do.
            let mut df = SessionContext::new().read_table(Arc::new(mem_table(
                self.schema.clone(),
                self.batches.clone(),
            )))?;
            for filter in filters {
                df = df.filter(filter.clone())?;
            }
            let batches = df.collect().await?;
            mem_table(self.schema.clone(), batches)
                .scan(state, projection, &[], limit)
                .await
        }
    }

    /// Hosts that each hold their own rows of every table.
    struct Hosts {
        hosts: Vec<SessionContext>,
        sql: std::sync::Mutex<Vec<String>>,
    }

    impl std::fmt::Debug for Hosts {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Hosts")
                .field("hosts", &self.hosts.len())
                .finish()
        }
    }

    impl CollectorTables for Hosts {
        fn is_collector_table(&self, provider: &dyn TableProvider) -> bool {
            provider.as_any().is::<HostTable>()
        }

        fn sql_table(&self, sql: String, schema: SchemaRef) -> Arc<dyn TableProvider> {
            self.sql.lock().unwrap().push(sql.clone());
            let batches: Vec<RecordBatch> = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let mut batches = Vec::new();
                    for host in &self.hosts {
                        batches.extend(host.sql(&sql).await.unwrap().collect().await.unwrap());
                    }
                    batches
                })
            });
            let batches = batches
                .into_iter()
                .map(|b| RecordBatch::try_new(schema.clone(), b.columns().to_vec()).unwrap())
                .collect();
            Arc::new(MemTable::try_new(schema, vec![batches]).unwrap())
        }
    }

    fn schemas() -> Vec<(&'static str, SchemaRef)> {
        use DataType::*;
        let schema = |fields: &[(&str, DataType, bool)]| {
            Arc::new(Schema::new(
                fields
                    .iter()
                    .map(|(n, t, null)| Field::new(*n, t.clone(), *null))
                    .collect::<Vec<_>>(),
            ))
        };
        vec![
            (
                "messages",
                schema(&[
                    ("id", UInt64, false),
                    ("to_actor_id", UInt64, false),
                    ("endpoint", Utf8, true),
                    ("port_index", UInt64, true),
                ]),
            ),
            (
                "message_status_events",
                schema(&[
                    ("id", UInt64, false),
                    ("message_id", UInt64, false),
                    ("status", Utf8, false),
                ]),
            ),
            (
                "actors",
                schema(&[("id", UInt64, false), ("mesh_id", UInt64, false)]),
            ),
            (
                "meshes",
                schema(&[("id", UInt64, false), ("given_name", Utf8, false)]),
            ),
        ]
    }

    /// Three hosts. Each records its actors' messages and their status
    /// events; host 0 also records every actor and mesh, as the spawning
    /// client does.
    fn host_rows() -> Vec<Vec<(&'static str, RecordBatch)>> {
        let schemas: std::collections::HashMap<_, _> = schemas().into_iter().collect();
        let u64s = |v: Vec<u64>| Arc::new(UInt64Array::from(v)) as _;
        let strs = |v: Vec<Option<&str>>| Arc::new(StringArray::from(v)) as _;
        let batch =
            |table: &str, columns| RecordBatch::try_new(schemas[table].clone(), columns).unwrap();
        (0..3u64)
            .map(|host| {
                let actors: Vec<u64> = (0..4).map(|a| host * 10 + a).collect();
                let mut ids = Vec::new();
                let mut to = Vec::new();
                let mut endpoints = Vec::new();
                let mut ports = Vec::new();
                let mut status_ids = Vec::new();
                let mut status_messages = Vec::new();
                let mut statuses = Vec::new();
                for (i, actor) in actors.iter().enumerate() {
                    for j in 0..(3 + i as u64 + host) {
                        let id = host * 1000 + (i as u64) * 100 + j;
                        ids.push(id);
                        to.push(*actor);
                        endpoints.push(if j % 4 == 0 {
                            Some("other")
                        } else if j % 5 == 0 {
                            None
                        } else {
                            Some("tick")
                        });
                        ports.push(if j % 3 == 0 { None } else { Some(j) });
                        for (k, status) in ["queued", "active", "complete"].iter().enumerate() {
                            if (j + k as u64) % 3 != 2 {
                                status_ids.push(id * 10 + k as u64);
                                status_messages.push(id);
                                statuses.push(Some(*status));
                            }
                        }
                    }
                }
                let ports: Vec<Option<u64>> = ports;
                let mut rows = vec![
                    (
                        "messages",
                        batch(
                            "messages",
                            vec![
                                u64s(ids),
                                u64s(to),
                                strs(endpoints),
                                Arc::new(UInt64Array::from(ports)) as _,
                            ],
                        ),
                    ),
                    (
                        "message_status_events",
                        batch(
                            "message_status_events",
                            vec![u64s(status_ids), u64s(status_messages), strs(statuses)],
                        ),
                    ),
                ];
                if host == 0 {
                    let all: Vec<u64> = (0..3)
                        .flat_map(|h| (0..4).map(move |a| h * 10 + a))
                        .collect();
                    let meshes: Vec<u64> = all.iter().map(|a| a % 2).collect();
                    rows.push(("actors", batch("actors", vec![u64s(all), u64s(meshes)])));
                    rows.push((
                        "meshes",
                        batch(
                            "meshes",
                            vec![u64s(vec![0, 1]), strs(vec![Some("load"), Some("other")])],
                        ),
                    ));
                }
                rows
            })
            .collect()
    }

    fn mem_table(schema: SchemaRef, batches: Vec<RecordBatch>) -> MemTable {
        MemTable::try_new(schema, vec![batches]).unwrap()
    }

    /// Returns a root context over the hosts, with or without the rule, and
    /// the SQL the rule pushed.
    fn root(push: bool) -> (SessionContext, Arc<Hosts>) {
        let rows = host_rows();
        let hosts: Vec<SessionContext> = rows
            .iter()
            .map(|host| {
                let ctx = SessionContext::new();
                for (table, schema) in schemas() {
                    let batches = host
                        .iter()
                        .filter(|(t, _)| *t == table)
                        .map(|(_, b)| b.clone())
                        .collect();
                    ctx.register_table(table, Arc::new(mem_table(schema, batches)))
                        .unwrap();
                }
                ctx
            })
            .collect();
        let hosts = Arc::new(Hosts {
            hosts,
            sql: Default::default(),
        });
        let mut state = SessionStateBuilder::new().with_default_features();
        if push {
            state = state
                .with_optimizer_rule(Arc::new(PushDownCollectorAggregates::new(hosts.clone())));
        }
        let ctx = SessionContext::new_with_state(state.build());
        for (table, schema) in schemas() {
            let batches = rows
                .iter()
                .flatten()
                .filter(|(t, _)| *t == table)
                .map(|(_, b)| b.clone())
                .collect();
            ctx.register_table(table, Arc::new(HostTable { schema, batches }))
                .unwrap();
        }
        (ctx, hosts)
    }

    async fn run(ctx: &SessionContext, sql: &str) -> String {
        let df = ctx.sql(sql).await.unwrap();
        let schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await.unwrap();
        let batch = concat_batches(&schema, &batches).unwrap();
        pretty_format_batches(&[batch]).unwrap().to_string()
    }

    const STATUS_SQL: &str = "SELECT mse.status, COUNT(*) AS status_count \
        FROM message_status_events mse \
        JOIN messages m ON mse.message_id = m.id \
        JOIN actors a ON m.to_actor_id = a.id \
        JOIN meshes mesh ON a.mesh_id = mesh.id \
        WHERE mesh.given_name = 'load' AND m.endpoint = 'tick' \
        GROUP BY mse.status ORDER BY mse.status";

    const MESSAGE_COUNT_SQL: &str = "SELECT COUNT(*) AS message_count \
        FROM messages m \
        JOIN actors a ON m.to_actor_id = a.id \
        JOIN meshes mesh ON a.mesh_id = mesh.id \
        WHERE mesh.given_name = 'load' AND m.endpoint = 'tick'";

    /// Asserts that `sql` returns the same rows with and without the rule,
    /// and returns the SQL the rule pushed.
    async fn assert_same(sql: &str) -> Vec<String> {
        let (plain, _) = root(false);
        let (pushed, hosts) = root(true);
        let expected = run(&plain, sql).await;
        let actual = run(&pushed, sql).await;
        assert_eq!(actual, expected, "{sql}");
        hosts.sql.lock().unwrap().clone()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_status_counts_push_join_to_collectors() {
        let pushed = assert_same(STATUS_SQL).await;
        assert_eq!(pushed.len(), 1, "{pushed:?}");
        assert!(pushed[0].contains("message_status_events"), "{}", pushed[0]);
        assert!(pushed[0].contains("JOIN"), "{}", pushed[0]);
        assert!(!pushed[0].contains("actors"), "{}", pushed[0]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_message_count_pushes_messages() {
        let pushed = assert_same(MESSAGE_COUNT_SQL).await;
        assert_eq!(pushed.len(), 1, "{pushed:?}");
        assert!(pushed[0].contains("FROM messages"), "{}", pushed[0]);
        assert!(!pushed[0].contains("actors"), "{}", pushed[0]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_aggregates_match_unpushed_results() {
        for sql in [
            "SELECT COUNT(*) FROM messages",
            "SELECT COUNT(DISTINCT to_actor_id) FROM messages",
            "SELECT COUNT(*) FROM messages WHERE endpoint = 'nothing'",
            "SELECT endpoint, COUNT(port_index), SUM(port_index), MIN(port_index), MAX(id) \
             FROM messages GROUP BY endpoint ORDER BY endpoint",
            "SELECT a.mesh_id, COUNT(*), SUM(m.port_index), MIN(mse.status), MAX(mse.id) \
             FROM message_status_events mse JOIN messages m ON m.id = mse.message_id \
             JOIN actors a ON a.id = m.to_actor_id GROUP BY a.mesh_id ORDER BY a.mesh_id",
            "SELECT COUNT(*) FROM actors a JOIN meshes mesh ON a.mesh_id = mesh.id \
             WHERE mesh.given_name = 'load'",
            "SELECT m.endpoint, COUNT(*) FROM messages m JOIN message_status_events mse \
             ON m.id = mse.message_id AND mse.status <> 'queued' GROUP BY m.endpoint ORDER BY 1",
        ] {
            assert_same(sql).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_joins_off_message_keys_are_not_pushed_together() {
        // Status events and messages meet on an actor id here, which different
        // hosts can share, so only one side is pushed.
        let pushed = assert_same(
            "SELECT COUNT(*) FROM messages m JOIN message_status_events mse \
             ON m.to_actor_id = mse.message_id",
        )
        .await;
        assert_eq!(pushed.len(), 1, "{pushed:?}");
        assert!(!pushed[0].contains("JOIN"), "{}", pushed[0]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unsupported_aggregates_and_small_tables_are_not_pushed() {
        for sql in [
            "SELECT AVG(port_index) FROM messages",
            "SELECT SUM(m.port_index * a.mesh_id) FROM messages m JOIN actors a ON m.to_actor_id = a.id",
            "SELECT COUNT(*) FROM actors a JOIN meshes mesh ON a.mesh_id = mesh.id \
             WHERE mesh.given_name = 'load'",
        ] {
            let pushed = assert_same(sql).await;
            assert!(pushed.is_empty(), "{sql}: {pushed:?}");
        }
    }
}
