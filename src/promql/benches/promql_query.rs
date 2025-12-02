use std::sync::Arc;
use std::time::Duration;

use api::prom_store::remote::{Label, Sample, TimeSeries, WriteRequest};
use arrow_array::RecordBatch;
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_query::{Output, OutputData};
use criterion::{Criterion, criterion_group, criterion_main};
use frontend::instance::Instance;
use futures::TryStreamExt;
#[cfg(target_os = "linux")]
use pprof::criterion::{Output as PProfOutput, PProfProfiler};
use query::parser::{DEFAULT_LOOKBACK_STRING, PromQuery};
use servers::prom_store;
use servers::query_handler::PromStoreProtocolHandler;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{Channel, QueryContext, QueryContextRef};
use tests_integration::standalone::{GreptimeDbStandalone, GreptimeDbStandaloneBuilder};
use tokio::runtime::Runtime;

const DB_NAME: &str = "promql_bench";
const START_MS: i64 = 1_700_000_000_000;
const STEP_MS: i64 = 1_000;
const SAMPLE_COUNT: usize = 10_000;

#[derive(Clone)]
struct PromqlBenchHarness {
    instance: Arc<Instance>,
    query_ctx: QueryContextRef,
    _standalone: Arc<GreptimeDbStandalone>,
}

impl PromqlBenchHarness {
    async fn new(db_name: &str) -> Self {
        let standalone = Arc::new(
            GreptimeDbStandaloneBuilder::new("promql_query_bench")
                .build()
                .await,
        );
        let instance = standalone.fe_instance().clone();
        let query_ctx = Arc::new(QueryContext::with_channel(
            DEFAULT_CATALOG_NAME,
            db_name,
            Channel::Promql,
        ));

        let create_database_sql = format!("CREATE DATABASE IF NOT EXISTS `{db_name}`");
        SqlQueryHandler::do_query(instance.as_ref(), &create_database_sql, query_ctx.clone())
            .await
            .into_iter()
            .for_each(|res| {
                res.expect("failed to create database for benchmark");
            });

        Self {
            instance,
            query_ctx,
            _standalone: standalone,
        }
    }

    async fn ingest_fixture(&self) {
        let write_request = build_write_request();
        let (row_inserts, _) = prom_store::to_grpc_row_insert_requests(&write_request)
            .expect("failed to convert write request");

        PromStoreProtocolHandler::write(
            self.instance.as_ref(),
            row_inserts,
            self.query_ctx.clone(),
            true,
        )
        .await
        .expect("failed to write fixture data");

        // for table in ["http_requests_total", "request_duration_seconds_bucket"] {
        //     let flush_sql = format!("admin flush_table('{table}')");
        //     SqlQueryHandler::do_query(self.instance.as_ref(), &flush_sql, self.query_ctx.clone())
        //         .await
        //         .into_iter()
        //         .for_each(|res| {
        //             res.expect("failed to flush table");
        //         });
        // }
    }

    async fn query(&self, prom_query: &PromQuery) -> Vec<RecordBatch> {
        let mut outputs = SqlQueryHandler::do_promql_query(
            self.instance.as_ref(),
            prom_query,
            self.query_ctx.clone(),
        )
        .await;
        let Output { data, .. } = outputs
            .pop()
            .expect("missing promql query output")
            .expect("promql query failed");

        match data {
            OutputData::Stream(stream) => stream
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .into_iter()
                .map(|batch| batch.into_df_record_batch())
                .collect(),
            OutputData::RecordBatches(batches) => batches
                .into_iter()
                .map(|batch| batch.into_df_record_batch())
                .collect(),
            other => panic!("unexpected query output: {other:?}"),
        }
    }
}

fn make_samples(start_ms: i64, step_ms: i64, count: usize, start_value: f64) -> Vec<Sample> {
    (0..count)
        .map(|i| Sample {
            value: start_value + i as f64,
            timestamp: start_ms + (i as i64 * step_ms),
        })
        .collect()
}

fn build_write_request() -> WriteRequest {
    let mut timeseries = Vec::new();

    let jobs = ["api", "web"];
    let instances = ["10.0.0.1:9100", "10.0.0.2:9100"];
    let methods = ["GET", "POST"];
    let statuses = ["200", "500"];

    for (job_idx, job) in jobs.iter().enumerate() {
        for (instance_idx, instance) in instances.iter().enumerate() {
            for (method_idx, method) in methods.iter().enumerate() {
                for (status_idx, status) in statuses.iter().enumerate() {
                    let start_value =
                        (job_idx + instance_idx + method_idx + status_idx) as f64 * 10.0;
                    timeseries.push(TimeSeries {
                        labels: vec![
                            Label {
                                name: "__name__".to_string(),
                                value: "http_requests_total".to_string(),
                            },
                            Label {
                                name: "job".to_string(),
                                value: job.to_string(),
                            },
                            Label {
                                name: "instance".to_string(),
                                value: instance.to_string(),
                            },
                            Label {
                                name: "method".to_string(),
                                value: method.to_string(),
                            },
                            Label {
                                name: "status".to_string(),
                                value: status.to_string(),
                            },
                        ],
                        samples: make_samples(START_MS, STEP_MS, SAMPLE_COUNT, start_value),
                        exemplars: vec![],
                    });
                }
            }
        }
    }

    let bucket_bounds = ["0.05", "0.1", "0.25", "0.5", "1", "2", "+Inf"];
    for (job_idx, job) in jobs.iter().enumerate() {
        for (instance_idx, instance) in instances.iter().enumerate() {
            for (bucket_idx, le) in bucket_bounds.iter().enumerate() {
                let start_value = (job_idx + instance_idx + bucket_idx) as f64;
                timeseries.push(TimeSeries {
                    labels: vec![
                        Label {
                            name: "__name__".to_string(),
                            value: "request_duration_seconds_bucket".to_string(),
                        },
                        Label {
                            name: "job".to_string(),
                            value: job.to_string(),
                        },
                        Label {
                            name: "instance".to_string(),
                            value: instance.to_string(),
                        },
                        Label {
                            name: "le".to_string(),
                            value: le.to_string(),
                        },
                    ],
                    samples: make_samples(START_MS, STEP_MS, SAMPLE_COUNT, start_value),
                    exemplars: vec![],
                });
            }
        }
    }

    WriteRequest {
        timeseries,
        metadata: vec![],
    }
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn bench_query(
    c: &mut Criterion,
    rt: &Runtime,
    harness: &PromqlBenchHarness,
    name: &str,
    prom_query: PromQuery,
) {
    let harness = harness.clone();
    c.bench_function(name, |b| {
        let harness = harness.clone();
        let prom_query = prom_query.clone();
        b.to_async(rt).iter(|| async {
            let batches = harness.query(&prom_query).await;
            assert!(total_rows(&batches) > 0);
        });
    });
}

fn build_queries() -> Vec<(&'static str, PromQuery)> {
    let start = START_MS / 1000;
    let end = start + (SAMPLE_COUNT as i64 * STEP_MS / 1000);

    vec![
        (
            "promql_instant_http_requests",
            PromQuery {
                query: r#"http_requests_total{job="api",method="GET"}"#.to_string(),
                start: start.to_string(),
                end: start.to_string(),
                step: "1s".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_rate_by_job",
            PromQuery {
                query: r#"rate(http_requests_total{job="api"}[1m])"#.to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "30s".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_sum_rate_job",
            PromQuery {
                query: "sum by (job) (rate(http_requests_total[1m]))".to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "1m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_sum_rate_method_status",
            PromQuery {
                query: "sum by (method, status) (rate(http_requests_total[5m]))".to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "2m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_histogram_quantile",
            PromQuery {
                query: "histogram_quantile(0.9, sum by (le) (rate(request_duration_seconds_bucket[5m])))"
                    .to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "5m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_increase_errors",
            PromQuery {
                query: r#"increase(http_requests_total{status="500"}[5m])"#.to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "2m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_error_ratio",
            PromQuery {
                query: "sum(rate(http_requests_total{status=\"500\"}[5m])) / sum(rate(http_requests_total[5m]))"
                    .to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "2m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_count_over_time_get",
            PromQuery {
                query: r#"count_over_time(http_requests_total{method="GET"}[2m])"#.to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "1m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_sum_increase_job",
            PromQuery {
                query: "sum by (job) (increase(http_requests_total[10m]))".to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "5m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_status_rate_web",
            PromQuery {
                query: "sum by (status) (rate(http_requests_total{job=\"web\"}[2m]))".to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "1m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_topk_instance",
            PromQuery {
                query: "topk(3, sum by (instance) (rate(http_requests_total[2m])))".to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "1m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_irate_instance",
            PromQuery {
                query: r#"irate(http_requests_total{instance="10.0.0.1:9100",method="GET"}[1m])"#.to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "30s".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_avg_irate_status",
            PromQuery {
                query: "avg by (status) (irate(http_requests_total{job=\"api\"}[1m]))".to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "30s".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_sum_over_time_post",
            PromQuery {
                query: r#"sum_over_time(http_requests_total{method="POST"}[5m])"#.to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "2m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_subquery_avg",
            PromQuery {
                query: "avg_over_time(rate(http_requests_total{job=\"web\"}[30s])[5m:30s])".to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "2m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_subquery_max_200s",
            PromQuery {
                query: "max_over_time(rate(http_requests_total{job=\"api\",status=\"200\"}[1m])[5m:1m])".to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "2m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_histogram_quantile_job",
            PromQuery {
                query: "histogram_quantile(0.99, sum by (job, le) (rate(request_duration_seconds_bucket[2m])))"
                    .to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "1m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_histogram_quantile_p95",
            PromQuery {
                query: "histogram_quantile(0.95, sum by (le) (rate(request_duration_seconds_bucket[5m])))"
                    .to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "2m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_sum_without",
            PromQuery {
                query: "sum without (instance, method) (rate(http_requests_total[1m]))".to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "1m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_absent_missing_job",
            PromQuery {
                query: "absent(http_requests_total{job=\"missing\"})".to_string(),
                start: start.to_string(),
                end: start.to_string(),
                step: "1s".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_regex_selector",
            PromQuery {
                query: r#"http_requests_total{job=~"api.*|web.*", method="GET"}"#.to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "1m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
        (
            "promql_group_left",
            PromQuery {
                query: r#"rate(http_requests_total[5m]) / on(job) group_left sum by (job) (rate(http_requests_total[5m]))"#.to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: "1m".to_string(),
                lookback: DEFAULT_LOOKBACK_STRING.to_string(),
                ..PromQuery::default()
            },
        ),
    ]
}

fn bench_promql(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let harness = rt.block_on(PromqlBenchHarness::new(DB_NAME));
    rt.block_on(harness.ingest_fixture());

    for (name, query) in build_queries() {
        bench_query(c, &rt, &harness, name, query);
    }
}

#[cfg(target_os = "linux")]
criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(1))
        .with_profiler(PProfProfiler::new(100, PProfOutput::Flamegraph(None)));
    targets = bench_promql
);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(1));
    targets = bench_promql
);

criterion_main!(benches);
