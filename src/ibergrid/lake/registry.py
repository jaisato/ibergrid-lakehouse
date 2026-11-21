"""The tables ``ibergrid lake init`` creates: Bronze, Silver and Gold medallion tables."""

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, MonthTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from ibergrid.lake.tables import TableSpec

# One row per REData point, for every widget (ADR 0003). The key is (dataset, electric_system,
# series_id, ts_utc); each ingestion run replaces one dataset, system and window.
REDATA_OBSERVATIONS = TableSpec(
    namespace="bronze",
    name="redata_observations",
    schema=Schema(
        NestedField(1, "dataset", StringType(), required=True, doc="ibergrid dataset name"),
        NestedField(2, "widget", StringType(), required=True, doc="REData widget path"),
        NestedField(3, "electric_system", StringType(), required=True),
        NestedField(4, "series_id", StringType(), required=True, doc="JSON:API id of the series"),
        NestedField(5, "series_type", StringType(), required=False),
        NestedField(6, "series_title", StringType(), required=False),
        NestedField(7, "granularity", StringType(), required=True, doc="5min, 15min, hour, day"),
        NestedField(8, "ts_utc", TimestamptzType(), required=True),
        NestedField(9, "ts_local", StringType(), required=True, doc="datetime as delivered"),
        NestedField(10, "value", DoubleType(), required=True),
        NestedField(11, "percentage", DoubleType(), required=False),
        NestedField(12, "series_last_update", TimestamptzType(), required=False),
        NestedField(13, "fetched_at", TimestamptzType(), required=True),
        NestedField(14, "run_id", StringType(), required=True),
        NestedField(15, "landing_path", StringType(), required=True, doc="raw response object"),
        NestedField(
            16, "payload_sha256", StringType(), required=True, doc="the answer's, without data.meta"
        ),
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dataset"),
        PartitionField(source_id=8, field_id=1001, transform=MonthTransform(), name="ts_utc_month"),
    ),
)

# One row per OMIE day-ahead period (ADR 0004). The key is (delivery_date, period); each
# ingestion run replaces whole delivery days. Partitioned by the month of delivery_date, the
# column every run replaces by, so a rerun drops whole files instead of rewriting a neighbour's.
OMIE_DAY_AHEAD_PRICES = TableSpec(
    namespace="bronze",
    name="omie_day_ahead_prices",
    schema=Schema(
        NestedField(1, "delivery_date", DateType(), required=True, doc="market day (Madrid)"),
        NestedField(2, "period", IntegerType(), required=True, doc="1-based, as in the file"),
        NestedField(3, "resolution_minutes", IntegerType(), required=True, doc="60 or 15"),
        NestedField(4, "interval_start_utc", TimestamptzType(), required=True),
        NestedField(5, "interval_end_utc", TimestamptzType(), required=True),
        NestedField(6, "price_es_eur_mwh", DoubleType(), required=True, doc="Spain, column 6"),
        NestedField(7, "price_pt_eur_mwh", DoubleType(), required=True, doc="Portugal, column 5"),
        NestedField(
            8, "contract_version", IntegerType(), required=True, doc="1 hourly, 2 quarter-hourly"
        ),
        NestedField(9, "source_file", StringType(), required=True, doc="OMIE's file name"),
        NestedField(
            10, "file_version", IntegerType(), required=True, doc="the N of the file name's .N"
        ),
        NestedField(11, "content_sha256", StringType(), required=True, doc="of the file's bytes"),
        NestedField(12, "fetched_at", TimestamptzType(), required=True),
        NestedField(13, "run_id", StringType(), required=True),
        NestedField(14, "landing_path", StringType(), required=True, doc="the original file"),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=MonthTransform(), name="delivery_date_month"
        ),
    ),
)

# One row per event the streaming lane accepted (ADR 0006), append-only: an event's key is
# (series_id, ts_utc), and a revision is a new row with another content_sha256; Silver (phase 3)
# keeps the latest: the highest (stream_epoch, kafka_offset), each `stream reset` adding an epoch.
# Kafka coordinates make each row traceable to its message, and the consumer's checkpoint lives in
# this table's properties. Partitioned like the batch table, by
# month: 1,150 points a day plus their revisions (REE reprograms the rest of the day every few
# minutes: tens of thousands of rows a day, ADR 0006) still make a day partition a file of a few
# megabytes after compaction (phase 4), and a month is what reads prune by. Manifest merging
# keeps hundreds of small appends a day from growing a manifest each.
REDATA_DEMAND_STREAM = TableSpec(
    namespace="bronze",
    name="redata_demand_stream",
    schema=Schema(
        NestedField(1, "series_id", StringType(), required=True, doc="JSON:API id of the series"),
        NestedField(2, "series_title", StringType(), required=False),
        NestedField(3, "electric_system", StringType(), required=True),
        NestedField(4, "ts_utc", TimestamptzType(), required=True),
        NestedField(5, "ts_local", StringType(), required=True, doc="datetime as delivered"),
        NestedField(6, "value", DoubleType(), required=True, doc="MW"),
        NestedField(7, "series_last_update", TimestamptzType(), required=False),
        NestedField(8, "content_sha256", StringType(), required=True, doc="the event's"),
        NestedField(9, "producer_instance", StringType(), required=True),
        NestedField(10, "producer_version", StringType(), required=True),
        NestedField(11, "emitted_at", TimestamptzType(), required=True),
        NestedField(12, "kafka_topic", StringType(), required=True),
        NestedField(13, "kafka_partition", IntegerType(), required=True),
        NestedField(14, "kafka_offset", LongType(), required=True),
        NestedField(15, "kafka_timestamp", TimestamptzType(), required=False),
        NestedField(16, "ingested_at", TimestamptzType(), required=True, doc="the flush's"),
        NestedField(17, "run_id", StringType(), required=True, doc="the flush's WAP run"),
        # Optional only so that ensure_table adds it to a table created before it existed.
        NestedField(18, "kafka_topic_id", StringType(), required=False, doc="topic incarnation"),
        NestedField(19, "stream_epoch", IntegerType(), required=False, doc="resets before the row"),
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=4, field_id=1000, transform=MonthTransform(), name="ts_utc_month"),
    ),
    properties={"commit.manifest-merge.enabled": "true"},
)

# --- Silver medallion tables (ADR 0007) ----------------------------------------------------------

# Reconciled 5-minute national demand: batch wins on closed Madrid days, deduplicated stream
# supplies open days (highest (coalesce(stream_epoch, 0), kafka_offset) per (series_id, ts_utc)).
SILVER_DEMAND_NATIONAL_5MIN = TableSpec(
    namespace="silver",
    name="demand_national_5min",
    schema=Schema(
        NestedField(1, "ts_utc", TimestamptzType(), required=True),
        NestedField(2, "madrid_date", DateType(), required=True),
        NestedField(3, "demand_real_mw", DoubleType(), required=False),
        NestedField(4, "demand_forecast_mw", DoubleType(), required=False),
        NestedField(5, "demand_scheduled_mw", DoubleType(), required=False),
        NestedField(6, "demand_total_scheduled_mw", DoubleType(), required=False),
        NestedField(7, "source_lane", StringType(), required=True, doc="batch or stream"),
        NestedField(8, "series_last_update_utc", TimestamptzType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=MonthTransform(), name="ts_utc_month"),
    ),
)

# Hourly demand per Spanish electric system, strictly separating peninsular and non-peninsular
# systems and flagging Canarias timestamps as unverified (ADR 0003).
SILVER_DEMAND_SYSTEM_HOURLY = TableSpec(
    namespace="silver",
    name="demand_system_hourly",
    schema=Schema(
        NestedField(1, "ts_utc", TimestamptzType(), required=True),
        NestedField(2, "madrid_date", DateType(), required=True),
        NestedField(3, "madrid_hour", IntegerType(), required=True),
        NestedField(4, "electric_system", StringType(), required=True),
        NestedField(
            5, "system_scope", StringType(), required=True, doc="peninsular or non_peninsular"
        ),
        NestedField(
            6,
            "ts_utc_verified",
            BooleanType(),
            required=True,
            doc="false for canarias pending clock confirmation (ADR 0003)",
        ),
        NestedField(7, "demand_mw", DoubleType(), required=True),
        NestedField(8, "series_last_update_utc", TimestamptzType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=MonthTransform(), name="ts_utc_month"),
    ),
)

# OMIE day-ahead MIBEL prices at native delivery resolution (v1 60 min, v2 15 min from 2025-10-01).
SILVER_MIBEL_PRICES_PERIOD = TableSpec(
    namespace="silver",
    name="mibel_prices_period",
    schema=Schema(
        NestedField(1, "delivery_date", DateType(), required=True),
        NestedField(2, "period", IntegerType(), required=True),
        NestedField(3, "resolution_minutes", IntegerType(), required=True),
        NestedField(4, "interval_start_utc", TimestamptzType(), required=True),
        NestedField(5, "interval_end_utc", TimestamptzType(), required=True),
        NestedField(6, "price_es_eur_mwh", DoubleType(), required=True),
        NestedField(7, "price_pt_eur_mwh", DoubleType(), required=True),
        NestedField(8, "spread_es_minus_pt_eur_mwh", DoubleType(), required=True),
        NestedField(9, "is_decoupled", BooleanType(), required=True),
        NestedField(10, "is_zero_or_negative_es", BooleanType(), required=True),
        NestedField(11, "is_zero_or_negative_pt", BooleanType(), required=True),
        NestedField(12, "contract_version", IntegerType(), required=True),
        NestedField(13, "file_version", IntegerType(), required=True),
        NestedField(14, "source_file", StringType(), required=True),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=MonthTransform(), name="delivery_date_month"
        ),
    ),
)

# Standardized hourly MIBEL price series across both contract eras, enriched with REData peninsular
# PVPC and spot price for cross-source reconciliation.
SILVER_MIBEL_PRICES_HOURLY = TableSpec(
    namespace="silver",
    name="mibel_prices_hourly",
    schema=Schema(
        NestedField(1, "hour_utc", TimestamptzType(), required=True),
        NestedField(2, "delivery_date", DateType(), required=True),
        NestedField(3, "madrid_hour", IntegerType(), required=True),
        NestedField(4, "contract_version", IntegerType(), required=True),
        NestedField(5, "periods_in_hour", IntegerType(), required=True),
        NestedField(6, "price_es_eur_mwh", DoubleType(), required=True),
        NestedField(7, "price_es_min_eur_mwh", DoubleType(), required=True),
        NestedField(8, "price_es_max_eur_mwh", DoubleType(), required=True),
        NestedField(9, "price_pt_eur_mwh", DoubleType(), required=True),
        NestedField(10, "spread_es_minus_pt_eur_mwh", DoubleType(), required=True),
        NestedField(11, "is_decoupled", BooleanType(), required=True),
        NestedField(12, "is_zero_or_negative_es", BooleanType(), required=True),
        NestedField(13, "pvpc_eur_mwh", DoubleType(), required=False),
        NestedField(14, "redata_spot_eur_mwh", DoubleType(), required=False),
        NestedField(15, "omie_redata_diff_eur_mwh", DoubleType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=MonthTransform(), name="hour_utc_month"
        ),
    ),
)

# Daily net generation per electric system and technology, excluding Total generation (10296) from
# technology rows while carrying reported_total_mwh so sums never double-count.
SILVER_GENERATION_DAILY = TableSpec(
    namespace="silver",
    name="generation_daily",
    schema=Schema(
        NestedField(1, "generation_date", DateType(), required=True),
        NestedField(2, "electric_system", StringType(), required=True),
        NestedField(
            3,
            "system_scope",
            StringType(),
            required=True,
            doc="national, peninsular or non_peninsular",
        ),
        NestedField(4, "series_id", StringType(), required=True),
        NestedField(5, "technology_slug", StringType(), required=True),
        NestedField(6, "technology_name", StringType(), required=True),
        NestedField(7, "energy_class", StringType(), required=True),
        NestedField(8, "is_renewable", BooleanType(), required=True),
        NestedField(9, "generation_mwh", DoubleType(), required=True),
        NestedField(10, "reported_total_mwh", DoubleType(), required=True),
        NestedField(11, "share_pct", DoubleType(), required=False),
        NestedField(12, "series_last_update_utc", TimestamptzType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=MonthTransform(), name="generation_date_month"
        ),
    ),
)

# --- Gold medallion tables (ADR 0007) ------------------------------------------------------------

# Peninsular hourly market mart: standardized OMIE ES/PT prices, peninsular-only hourly demand,
# 1-hour ramps, intraday price rank, daily peninsular renewable shares and attribution.
GOLD_MIBEL_HOURLY_MARKET = TableSpec(
    namespace="gold",
    name="mibel_hourly_market",
    schema=Schema(
        NestedField(1, "hour_utc", TimestamptzType(), required=True),
        NestedField(2, "delivery_date", DateType(), required=True),
        NestedField(3, "madrid_hour", IntegerType(), required=True),
        NestedField(4, "day_type", StringType(), required=True, doc="weekday or weekend"),
        NestedField(5, "price_es_eur_mwh", DoubleType(), required=True),
        NestedField(6, "price_pt_eur_mwh", DoubleType(), required=True),
        NestedField(7, "spread_es_minus_pt_eur_mwh", DoubleType(), required=True),
        NestedField(8, "is_decoupled", BooleanType(), required=True),
        NestedField(9, "is_zero_or_negative_es", BooleanType(), required=True),
        NestedField(10, "pvpc_eur_mwh", DoubleType(), required=False),
        NestedField(11, "peninsular_demand_mw", DoubleType(), required=False),
        NestedField(12, "demand_ramp_1h_mw", DoubleType(), required=False),
        NestedField(13, "price_ramp_1h_eur_mwh", DoubleType(), required=False),
        NestedField(14, "intraday_price_rank", IntegerType(), required=True),
        NestedField(15, "is_evening_ramp_window", BooleanType(), required=True),
        NestedField(16, "peninsular_renewable_share_pct", DoubleType(), required=False),
        NestedField(17, "peninsular_solar_share_pct", DoubleType(), required=False),
        NestedField(18, "peninsular_wind_share_pct", DoubleType(), required=False),
        NestedField(19, "source_attribution", StringType(), required=True),
        NestedField(20, "updated_at_utc", TimestamptzType(), required=True),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=MonthTransform(), name="hour_utc_month"
        ),
    ),
)

# Daily MIBEL executive summary (Madrid day): peninsular and non-peninsular demand kept in separate
# columns and never mixed.
GOLD_MIBEL_DAILY_SUMMARY = TableSpec(
    namespace="gold",
    name="mibel_daily_summary",
    schema=Schema(
        NestedField(1, "delivery_date", DateType(), required=True),
        NestedField(2, "hours_in_day", IntegerType(), required=True),
        NestedField(3, "price_es_mean_eur_mwh", DoubleType(), required=True),
        NestedField(4, "price_es_min_eur_mwh", DoubleType(), required=True),
        NestedField(5, "price_es_max_eur_mwh", DoubleType(), required=True),
        NestedField(6, "price_es_peak_eur_mwh", DoubleType(), required=False),
        NestedField(7, "price_es_offpeak_eur_mwh", DoubleType(), required=False),
        NestedField(8, "price_pt_mean_eur_mwh", DoubleType(), required=True),
        NestedField(9, "spread_mean_eur_mwh", DoubleType(), required=True),
        NestedField(10, "spread_abs_mean_eur_mwh", DoubleType(), required=True),
        NestedField(11, "decoupled_hours", IntegerType(), required=True),
        NestedField(12, "zero_or_negative_es_hours", IntegerType(), required=True),
        NestedField(13, "peninsular_demand_mwh", DoubleType(), required=False),
        NestedField(14, "peninsular_peak_demand_mw", DoubleType(), required=False),
        NestedField(15, "evening_ramp_max_mw", DoubleType(), required=False),
        NestedField(16, "non_peninsular_demand_mwh", DoubleType(), required=False),
        NestedField(17, "peninsular_renewable_share_pct", DoubleType(), required=False),
        NestedField(18, "peninsular_solar_share_pct", DoubleType(), required=False),
        NestedField(19, "peninsular_wind_share_pct", DoubleType(), required=False),
        NestedField(20, "source_attribution", StringType(), required=True),
        NestedField(21, "updated_at_utc", TimestamptzType(), required=True),
    ),
)

# Daily demand and renewable share per electric system (peninsular + four non-peninsular systems).
GOLD_SYSTEM_DEMAND_DAILY = TableSpec(
    namespace="gold",
    name="system_demand_daily",
    schema=Schema(
        NestedField(1, "demand_date", DateType(), required=True),
        NestedField(2, "electric_system", StringType(), required=True),
        NestedField(3, "system_scope", StringType(), required=True),
        NestedField(4, "ts_utc_verified", BooleanType(), required=True),
        NestedField(5, "hours_in_day", IntegerType(), required=True),
        NestedField(6, "demand_total_mwh", DoubleType(), required=True),
        NestedField(7, "demand_mean_mw", DoubleType(), required=True),
        NestedField(8, "demand_min_mw", DoubleType(), required=True),
        NestedField(9, "demand_peak_mw", DoubleType(), required=True),
        NestedField(10, "generation_total_mwh", DoubleType(), required=False),
        NestedField(11, "renewable_generation_mwh", DoubleType(), required=False),
        NestedField(12, "renewable_share_pct", DoubleType(), required=False),
        NestedField(13, "solar_share_pct", DoubleType(), required=False),
        NestedField(14, "wind_share_pct", DoubleType(), required=False),
        NestedField(15, "source_attribution", StringType(), required=True),
        NestedField(16, "updated_at_utc", TimestamptzType(), required=True),
    ),
)

# Calendar-month x day-type x Madrid-hour profile for industrial / data-center load shifting.
GOLD_OPTIMAL_CONSUMPTION_WINDOWS = TableSpec(
    namespace="gold",
    name="optimal_consumption_windows",
    schema=Schema(
        NestedField(1, "calendar_month", IntegerType(), required=True),
        NestedField(2, "day_type", StringType(), required=True),
        NestedField(3, "madrid_hour", IntegerType(), required=True),
        NestedField(4, "sample_hours", IntegerType(), required=True),
        NestedField(5, "price_es_mean_eur_mwh", DoubleType(), required=True),
        NestedField(6, "price_es_p10_eur_mwh", DoubleType(), required=True),
        NestedField(7, "price_es_p50_eur_mwh", DoubleType(), required=True),
        NestedField(8, "price_es_p90_eur_mwh", DoubleType(), required=True),
        NestedField(9, "zero_or_negative_es_pct", DoubleType(), required=True),
        NestedField(10, "decoupled_pct", DoubleType(), required=True),
        NestedField(11, "peninsular_demand_mean_mw", DoubleType(), required=False),
        NestedField(12, "peninsular_renewable_share_mean_pct", DoubleType(), required=False),
        NestedField(13, "window_rank", IntegerType(), required=True),
        NestedField(14, "is_optimal_4h_window", BooleanType(), required=True),
        NestedField(15, "source_attribution", StringType(), required=True),
        NestedField(16, "updated_at_utc", TimestamptzType(), required=True),
    ),
)

BRONZE_TABLES: tuple[TableSpec, ...] = (
    REDATA_OBSERVATIONS,
    OMIE_DAY_AHEAD_PRICES,
    REDATA_DEMAND_STREAM,
)
SILVER_TABLES: tuple[TableSpec, ...] = (
    SILVER_DEMAND_NATIONAL_5MIN,
    SILVER_DEMAND_SYSTEM_HOURLY,
    SILVER_MIBEL_PRICES_PERIOD,
    SILVER_MIBEL_PRICES_HOURLY,
    SILVER_GENERATION_DAILY,
)
GOLD_TABLES: tuple[TableSpec, ...] = (
    GOLD_MIBEL_HOURLY_MARKET,
    GOLD_MIBEL_DAILY_SUMMARY,
    GOLD_SYSTEM_DEMAND_DAILY,
    GOLD_OPTIMAL_CONSUMPTION_WINDOWS,
)
MEDALLION_TABLES: tuple[TableSpec, ...] = (*SILVER_TABLES, *GOLD_TABLES)
TABLES: tuple[TableSpec, ...] = (*BRONZE_TABLES, *SILVER_TABLES, *GOLD_TABLES)
