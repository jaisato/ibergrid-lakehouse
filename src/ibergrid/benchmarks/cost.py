"""Real European monthly cloud cost estimator for IberGrid Lakehouse (ADR 0009).

Models published Cloudflare R2 Standard Storage pricing (10 GB free tier, $0.015/GB-month above,
$0.00 egress, 1M Class A and 10M Class B free operations/month) combined with European Arm64 VPS
compute (Hetzner Cloud Falkenstein/Helsinki) versus a managed Apache Spark stack.
"""

from __future__ import annotations

from dataclasses import dataclass

# Published Cloudflare R2 Standard Storage rates (USD).
R2_FREE_STORAGE_GB = 10.0
R2_STORAGE_USD_PER_GB = 0.015
R2_FREE_CLASS_A_OPS = 1_000_000
R2_CLASS_A_USD_PER_MILLION = 4.50
R2_FREE_CLASS_B_OPS = 10_000_000
R2_CLASS_B_USD_PER_MILLION = 0.36
USD_TO_EUR = 0.92


@dataclass(frozen=True)
class R2UsageEstimate:
    """Monthly Cloudflare R2 cost breakdown for a given dataset footprint and op volume."""

    label: str
    storage_gb: float
    monthly_class_a_ops: int
    monthly_class_b_ops: int
    monthly_egress_gb: float
    storage_usd: float
    class_a_usd: float
    class_b_usd: float
    egress_usd: float
    total_usd: float
    total_eur: float


@dataclass(frozen=True)
class InfrastructureScenario:
    """End-to-end European monthly cost comparison scenario."""

    name: str
    compute_tier: str
    storage_tier: str
    compute_eur_month: float
    storage_eur_month: float
    egress_eur_month: float
    total_eur_month: float
    notes: str


def estimate_r2_monthly_cost(
    *,
    label: str,
    storage_bytes: int,
    monthly_class_a_ops: int,
    monthly_class_b_ops: int,
    monthly_egress_gb: float = 100.0,
    usd_to_eur: float = USD_TO_EUR,
) -> R2UsageEstimate:
    """Compute monthly Cloudflare R2 Standard Storage cost including the free tier."""
    if storage_bytes < 0:
        raise ValueError("storage_bytes must be >= 0")
    if monthly_class_a_ops < 0 or monthly_class_b_ops < 0:
        raise ValueError("operation counts must be >= 0")
    if monthly_egress_gb < 0:
        raise ValueError("monthly_egress_gb must be >= 0")

    storage_gb = storage_bytes / (1024.0**3)
    billable_gb = max(0.0, storage_gb - R2_FREE_STORAGE_GB)
    billable_a = max(0, monthly_class_a_ops - R2_FREE_CLASS_A_OPS)
    billable_b = max(0, monthly_class_b_ops - R2_FREE_CLASS_B_OPS)

    storage_usd = round(billable_gb * R2_STORAGE_USD_PER_GB, 4)
    class_a_usd = round((billable_a / 1_000_000.0) * R2_CLASS_A_USD_PER_MILLION, 4)
    class_b_usd = round((billable_b / 1_000_000.0) * R2_CLASS_B_USD_PER_MILLION, 4)
    total_usd = round(storage_usd + class_a_usd + class_b_usd, 2)
    total_eur = round(total_usd * usd_to_eur, 2)

    return R2UsageEstimate(
        label=label,
        storage_gb=round(storage_gb, 4),
        monthly_class_a_ops=monthly_class_a_ops,
        monthly_class_b_ops=monthly_class_b_ops,
        monthly_egress_gb=round(monthly_egress_gb, 2),
        storage_usd=storage_usd,
        class_a_usd=class_a_usd,
        class_b_usd=class_b_usd,
        egress_usd=0.0,
        total_usd=total_usd,
        total_eur=total_eur,
    )


def project_ibergrid_monthly_ops(
    *,
    daily_batch_writes: int = 35,
    daily_stream_flushes: int = 1440,
    daily_medallion_runs: int = 2,
    daily_maintenance_runs: int = 1,
    daily_api_and_portal_reads: int = 5000,
) -> tuple[int, int]:
    """Conservative monthly (Class A, Class B) R2 operation projection for IberGrid."""
    # Each write/flush writes data + manifest + manifest-list + metadata.json (~4 Class A ops).
    class_a_per_day = (
        daily_batch_writes * 5
        + daily_stream_flushes * 4
        + daily_medallion_runs * 60
        + daily_maintenance_runs * 40
    )
    # Reads cover metadata scans, manifest reads, dbt inputs, quality audits and serving traffic.
    class_b_per_day = (
        daily_batch_writes * 10
        + daily_stream_flushes * 6
        + daily_medallion_runs * 300
        + daily_maintenance_runs * 100
        + daily_api_and_portal_reads * 3
    )
    return class_a_per_day * 30, class_b_per_day * 30


def build_cost_comparison(
    measured_storage_bytes: int,
) -> tuple[tuple[R2UsageEstimate, ...], tuple[InfrastructureScenario, ...]]:
    """Build R2 scaling tiers and EU monthly infrastructure comparison scenarios."""
    base_class_a, base_class_b = project_ibergrid_monthly_ops()
    r2_tiers = (
        estimate_r2_monthly_cost(
            label="Actual 3.9-year IberGrid dataset (1x)",
            storage_bytes=measured_storage_bytes,
            monthly_class_a_ops=base_class_a,
            monthly_class_b_ops=base_class_b,
            monthly_egress_gb=50.0,
        ),
        estimate_r2_monthly_cost(
            label="10-year + high-frequency expansion (~10 GB)",
            storage_bytes=10 * 1024**3,
            monthly_class_a_ops=base_class_a * 2,
            monthly_class_b_ops=base_class_b * 3,
            monthly_egress_gb=250.0,
        ),
        estimate_r2_monthly_cost(
            label="Pan-European ENTSO-E scale (~100 GB)",
            storage_bytes=100 * 1024**3,
            monthly_class_a_ops=base_class_a * 5,
            monthly_class_b_ops=base_class_b * 10,
            monthly_egress_gb=1000.0,
        ),
        estimate_r2_monthly_cost(
            label="Sub-terabyte ceiling (1 TB)",
            storage_bytes=1024 * 1024**3,
            monthly_class_a_ops=base_class_a * 10,
            monthly_class_b_ops=base_class_b * 20,
            monthly_egress_gb=2000.0,
        ),
    )

    r2_actual_eur = r2_tiers[0].total_eur
    scenarios = (
        InfrastructureScenario(
            name="IberGrid minimal EU (batch + serving)",
            compute_tier="Hetzner Cloud CAX11 (2 Arm64 vCPU, 4 GB RAM, 40 GB NVMe, Falkenstein)",
            storage_tier="Cloudflare R2 Standard (EU jurisdiction, 0 egress)",
            compute_eur_month=3.79,
            storage_eur_month=r2_actual_eur,
            egress_eur_month=0.0,
            total_eur_month=round(3.79 + r2_actual_eur, 2),
            notes="Ex-VAT list price (~4.59 EUR incl. 21 % ES VAT); 20 TB traffic included.",
        ),
        InfrastructureScenario(
            name="IberGrid full stack EU (batch + Redpanda + Dagster + API)",
            compute_tier="Hetzner Cloud CAX21 (4 Arm64 vCPU, 8 GB RAM, 80 GB NVMe, Falkenstein)",
            storage_tier="Cloudflare R2 Standard (EU jurisdiction, 0 egress)",
            compute_eur_month=6.49,
            storage_eur_month=r2_actual_eur,
            egress_eur_month=0.0,
            total_eur_month=round(6.49 + r2_actual_eur, 2),
            notes=(
                "Ex-VAT list price (~7.85 EUR incl. 21 % ES VAT); comfortably fits Lakekeeper, "
                "Postgres, Redpanda, Dagster, dbt, Soda and FastAPI."
            ),
        ),
        InfrastructureScenario(
            name="Managed Spark baseline (reference, EU region)",
            compute_tier=(
                "Managed Spark cluster / Databricks Jobs + Managed Kafka + RDS Postgres catalog"
            ),
            storage_tier="AWS S3 Standard + NAT/egress",
            compute_eur_month=340.0,
            storage_eur_month=2.5,
            egress_eur_month=18.0,
            total_eur_month=360.5,
            notes=(
                "Conservative lower bound for always-on/scheduled EU managed Spark + broker + "
                "catalog; 30-90 s JVM cold starts and ~55x higher fixed cost for <1 TB data."
            ),
        ),
    )
    return r2_tiers, scenarios
