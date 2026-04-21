import argparse
import os
from datetime import datetime
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
from faker import Faker


# -----------------------------------------------------------------------------
# Configuration block
# -----------------------------------------------------------------------------
DEFAULT_CONFIG: Dict[str, Any] = {
    "num_rows": 2_000_000,
    "seed": 42,
    "output_path": "pos_data.csv",
    "batch_size": 250_000,
    "progress_every": 250_000,
    "date_start": "2025-01-01",
    "date_end": "2026-12-31",
}


# Use unicode escapes so the source file stays ASCII while data remains Vietnamese.
REGION_NORTH = "Mi\u1ec1n B\u1eafc"
REGION_CENTRAL = "Mi\u1ec1n Trung"
REGION_SOUTH = "Mi\u1ec1n Nam"

REGION_CITY_MAP: Dict[str, list[str]] = {
    REGION_NORTH: [
        "H\u00e0 N\u1ed9i",
        "H\u1ea3i Ph\u00f2ng",
        "Qu\u1ea3ng Ninh",
        "B\u1eafc Ninh",
    ],
    REGION_CENTRAL: [
        "\u0110\u00e0 N\u1eb5ng",
        "Hu\u1ebf",
        "Ngh\u1ec7 An",
        "Nha Trang",
    ],
    REGION_SOUTH: [
        "TP.HCM",
        "C\u1ea7n Th\u01a1",
        "B\u00ecnh D\u01b0\u01a1ng",
        "\u0110\u1ed3ng Nai",
    ],
}

REGIONS = np.array([REGION_NORTH, REGION_SOUTH, REGION_CENTRAL], dtype=object)
REGION_WEIGHTS = np.array([0.45, 0.40, 0.15], dtype=np.float64)
REGION_TARGET_DISTRIBUTION: Dict[str, float] = {
    REGION_NORTH: 0.45,
    REGION_SOUTH: 0.40,
    REGION_CENTRAL: 0.15,
}

CUSTOMER_TYPES = np.array(["VIP", "Normal"], dtype=object)
CUSTOMER_TYPE_WEIGHTS = np.array([0.20, 0.80], dtype=np.float64)
CUSTOMER_TYPE_TARGET_DISTRIBUTION: Dict[str, float] = {"VIP": 0.20, "Normal": 0.80}

CATEGORIES = np.array(
    ["Electronics", "F&B", "Fashion", "Grocery", "Home Appliances"],
    dtype=object,
)
CATEGORY_INDEX = {name: idx for idx, name in enumerate(CATEGORIES.tolist())}

# Category distribution by customer type.
CATEGORY_BASE_BY_CUSTOMER: Dict[str, np.ndarray] = {
    "VIP": np.array([0.55, 0.10, 0.20, 0.00, 0.15], dtype=np.float64),
    "Normal": np.array([0.20, 0.40, 0.25, 0.10, 0.05], dtype=np.float64),
}

# Region impact on category probabilities (light skew).
REGION_CATEGORY_MULTIPLIERS: Dict[str, np.ndarray] = {
    REGION_NORTH: np.array([1.10, 0.95, 1.08, 0.95, 1.00], dtype=np.float64),
    REGION_SOUTH: np.array([0.95, 1.08, 0.95, 1.15, 1.00], dtype=np.float64),
    REGION_CENTRAL: np.array([1.00, 1.00, 1.00, 1.00, 0.85], dtype=np.float64),
}

PAYMENT_METHODS = np.array(["Cash", "Credit Card", "Bank Transfer", "E-Wallet"], dtype=object)
PAYMENT_PROBS_BY_CATEGORY: Dict[str, np.ndarray] = {
    "Electronics": np.array([0.10, 0.45, 0.25, 0.20], dtype=np.float64),
    "F&B": np.array([0.45, 0.15, 0.10, 0.30], dtype=np.float64),
    "Fashion": np.array([0.20, 0.30, 0.15, 0.35], dtype=np.float64),
    "Grocery": np.array([0.50, 0.15, 0.10, 0.25], dtype=np.float64),
    "Home Appliances": np.array([0.10, 0.40, 0.30, 0.20], dtype=np.float64),
}

QUANTITY_RULES: Dict[str, Dict[str, np.ndarray]] = {
    "Electronics": {
        "values": np.array([1, 2], dtype=np.int16),
        "probs": np.array([0.75, 0.25], dtype=np.float64),
    },
    "Home Appliances": {
        "values": np.array([1, 2], dtype=np.int16),
        "probs": np.array([0.70, 0.30], dtype=np.float64),
    },
    "Fashion": {
        "values": np.array([1, 2, 3, 4], dtype=np.int16),
        "probs": np.array([0.40, 0.30, 0.20, 0.10], dtype=np.float64),
    },
    "Grocery": {
        "values": np.array([1, 2, 3, 4, 5, 6, 7, 8], dtype=np.int16),
        "probs": np.array([0.25, 0.20, 0.16, 0.12, 0.10, 0.08, 0.06, 0.03], dtype=np.float64),
    },
    "F&B": {
        "values": np.array([1, 2, 3, 4, 5, 6], dtype=np.int16),
        "probs": np.array([0.30, 0.25, 0.18, 0.14, 0.09, 0.04], dtype=np.float64),
    },
}

SALES_RULES: Dict[str, Dict[str, float]] = {
    "Electronics": {
        "sales_min": 3_000_000.0,
        "sales_max": 40_000_000.0,
        "unit_left": 2_800_000.0,
        "unit_mode": 8_500_000.0,
        "unit_right": 23_000_000.0,
    },
    "Home Appliances": {
        "sales_min": 2_000_000.0,
        "sales_max": 25_000_000.0,
        "unit_left": 1_800_000.0,
        "unit_mode": 5_500_000.0,
        "unit_right": 14_000_000.0,
    },
    "Fashion": {
        "sales_min": 200_000.0,
        "sales_max": 3_000_000.0,
        "unit_left": 90_000.0,
        "unit_mode": 350_000.0,
        "unit_right": 1_100_000.0,
    },
    "Grocery": {
        "sales_min": 50_000.0,
        "sales_max": 1_000_000.0,
        "unit_left": 12_000.0,
        "unit_mode": 65_000.0,
        "unit_right": 220_000.0,
    },
    "F&B": {
        "sales_min": 30_000.0,
        "sales_max": 800_000.0,
        "unit_left": 10_000.0,
        "unit_mode": 45_000.0,
        "unit_right": 180_000.0,
    },
}


def generate_transaction_ids(start_index: int, batch_size: int) -> np.ndarray:
    """Generate unique transaction IDs with fixed width format: TXN-0000001."""
    txn_numbers = np.arange(start_index, start_index + batch_size, dtype=np.int64)
    return np.char.add("TXN-", np.char.zfill(txn_numbers.astype(str), 7))


def generate_dates(batch_size: int, rng: np.random.Generator, start_date: str, end_date: str) -> np.ndarray:
    """Generate random transaction dates within a closed date range."""
    start_dt = np.datetime64(start_date, "D")
    end_dt = np.datetime64(end_date, "D")
    num_days = int((end_dt - start_dt).astype(np.int64)) + 1
    offsets = rng.integers(0, num_days, size=batch_size, dtype=np.int32)
    return start_dt + offsets.astype("timedelta64[D]")


def generate_regions_and_cities(batch_size: int, rng: np.random.Generator) -> Tuple[np.ndarray, np.ndarray]:
    """Generate regions with skewed distribution and valid cities constrained by region."""
    regions = rng.choice(REGIONS, size=batch_size, p=REGION_WEIGHTS)
    cities = np.empty(batch_size, dtype=object)

    for region in REGIONS.tolist():
        mask = regions == region
        count = int(mask.sum())
        if count > 0:
            cities[mask] = rng.choice(REGION_CITY_MAP[region], size=count)

    return regions, cities


def generate_customer_types(batch_size: int, rng: np.random.Generator) -> np.ndarray:
    """Generate customer types based on target distribution."""
    return rng.choice(CUSTOMER_TYPES, size=batch_size, p=CUSTOMER_TYPE_WEIGHTS)


def _seasonal_multipliers(dates: np.ndarray) -> np.ndarray:
    """Create row-level category multipliers by month/weekend/holiday for light seasonality."""
    date_index = pd.DatetimeIndex(dates.astype("datetime64[ns]"))
    months = date_index.month.to_numpy()
    days = date_index.day.to_numpy()
    weekdays = date_index.dayofweek.to_numpy()

    multipliers = np.ones((dates.shape[0], len(CATEGORIES)), dtype=np.float64)

    multipliers[np.isin(months, [10, 11, 12]), CATEGORY_INDEX["Electronics"]] *= 1.15
    multipliers[np.isin(months, [11, 12]), CATEGORY_INDEX["Electronics"]] *= 1.10
    multipliers[np.isin(months, [3, 4, 8, 9, 11, 12]), CATEGORY_INDEX["Fashion"]] *= 1.12
    multipliers[weekdays >= 5, CATEGORY_INDEX["F&B"]] *= 1.18
    multipliers[np.isin(months, [1, 2]), CATEGORY_INDEX["F&B"]] *= 1.08

    holiday_mask = (
        ((months == 1) & (days == 1))
        | ((months == 4) & (days == 30))
        | ((months == 5) & (days == 1))
        | ((months == 9) & (days == 2))
        | ((months == 12) & np.isin(days, [24, 31]))
    )
    multipliers[holiday_mask, CATEGORY_INDEX["F&B"]] *= 1.25

    return multipliers


def _sample_from_probability_matrix(
    probability_matrix: np.ndarray,
    rng: np.random.Generator,
    labels: np.ndarray,
) -> np.ndarray:
    """Vectorized sampling where each row has its own probability vector."""
    cumulative = np.cumsum(probability_matrix, axis=1)
    random_values = rng.random(probability_matrix.shape[0])[:, None]
    sampled_idx = np.sum(random_values > cumulative, axis=1)
    sampled_idx = np.minimum(sampled_idx, probability_matrix.shape[1] - 1)
    return labels[sampled_idx]


def generate_categories(
    customer_types: np.ndarray,
    regions: np.ndarray,
    dates: np.ndarray,
    rng: np.random.Generator,
) -> np.ndarray:
    """
    Generate categories with multi-factor correlation:
    - customer_type -> category base probabilities
    - region -> category skew multipliers
    - date -> seasonal multipliers
    """
    batch_size = customer_types.shape[0]
    probs = np.empty((batch_size, len(CATEGORIES)), dtype=np.float64)

    vip_mask = customer_types == "VIP"
    probs[vip_mask, :] = CATEGORY_BASE_BY_CUSTOMER["VIP"]
    probs[~vip_mask, :] = CATEGORY_BASE_BY_CUSTOMER["Normal"]

    for region, multiplier in REGION_CATEGORY_MULTIPLIERS.items():
        region_mask = regions == region
        if np.any(region_mask):
            probs[region_mask, :] *= multiplier

    probs *= _seasonal_multipliers(dates)

    row_sums = probs.sum(axis=1, keepdims=True)
    probs = probs / row_sums

    return _sample_from_probability_matrix(probs, rng, CATEGORIES)


def generate_payment_methods(categories: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Generate payment methods conditional on category."""
    payment_methods = np.empty(categories.shape[0], dtype=object)

    for category in CATEGORIES.tolist():
        mask = categories == category
        count = int(mask.sum())
        if count > 0:
            payment_methods[mask] = rng.choice(
                PAYMENT_METHODS,
                size=count,
                p=PAYMENT_PROBS_BY_CATEGORY[category],
            )

    return payment_methods


def generate_quantity_and_sales(categories: np.ndarray, rng: np.random.Generator) -> Tuple[np.ndarray, np.ndarray]:
    """
    Generate Quantity and Sales_Amount per category.
    Sales_Amount depends on Quantity * unit_price * noise and is clipped to category bounds.
    """
    quantity = np.empty(categories.shape[0], dtype=np.int16)
    sales_amount = np.empty(categories.shape[0], dtype=np.float64)

    for category in CATEGORIES.tolist():
        mask = categories == category
        count = int(mask.sum())
        if count == 0:
            continue

        qty_values = QUANTITY_RULES[category]["values"]
        qty_probs = QUANTITY_RULES[category]["probs"]
        qty = rng.choice(qty_values, size=count, p=qty_probs).astype(np.int16)
        quantity[mask] = qty

        rule = SALES_RULES[category]
        unit_price = rng.triangular(
            left=rule["unit_left"],
            mode=rule["unit_mode"],
            right=rule["unit_right"],
            size=count,
        )
        noise = np.clip(rng.normal(loc=1.0, scale=0.08, size=count), 0.85, 1.20)
        sales = qty.astype(np.float64) * unit_price * noise
        sales = np.clip(sales, rule["sales_min"], rule["sales_max"])
        sales = np.round(sales / 1000.0) * 1000.0

        sales_amount[mask] = sales

    return quantity, sales_amount.astype(np.float32)


def build_dataframe(
    transaction_ids: np.ndarray,
    dates: np.ndarray,
    regions: np.ndarray,
    cities: np.ndarray,
    categories: np.ndarray,
    customer_types: np.ndarray,
    payment_methods: np.ndarray,
    sales_amount: np.ndarray,
    quantity: np.ndarray,
) -> pd.DataFrame:
    """Build DataFrame with required schema and memory-friendly dtypes."""
    df = pd.DataFrame(
        {
            "Transaction_ID": transaction_ids,
            "Date": np.datetime_as_string(dates, unit="D"),
            "Region": regions,
            "City": cities,
            "Category": categories,
            "Customer_Type": customer_types,
            "Payment_Method": payment_methods,
            "Sales_Amount": sales_amount.astype(np.float32),
            "Quantity": quantity.astype(np.int16),
        }
    )

    for col in ["Region", "City", "Category", "Customer_Type", "Payment_Method"]:
        df[col] = pd.Categorical(df[col])

    return df


def save_to_csv(df: pd.DataFrame, output_path: str, mode: str = "a", write_header: bool = False) -> None:
    """Append a batch to CSV output file."""
    output_dir = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_path, mode=mode, header=write_header, index=False, encoding="utf-8-sig")


def _update_counter(counter: Dict[str, int], values: np.ndarray) -> None:
    unique_values, counts = np.unique(values, return_counts=True)
    for value, count in zip(unique_values.tolist(), counts.tolist()):
        counter[str(value)] = counter.get(str(value), 0) + int(count)


def _safe_ratio(numerator: float, denominator: float) -> float:
    return float(numerator / denominator) if denominator else 0.0


def _count_invalid_city_region(regions: np.ndarray, cities: np.ndarray) -> int:
    invalid = 0
    for region, valid_cities in REGION_CITY_MAP.items():
        region_mask = regions == region
        if np.any(region_mask):
            invalid += int((~np.isin(cities[region_mask], valid_cities)).sum())
    return invalid


def _init_validation_state() -> Dict[str, Any]:
    return {
        "total_rows": 0,
        "head_df": None,
        "region_counts": {region: 0 for region in REGIONS.tolist()},
        "customer_counts": {customer: 0 for customer in CUSTOMER_TYPES.tolist()},
        "category_counts": {category: 0 for category in CATEGORIES.tolist()},
        "payment_counts": {method: 0 for method in PAYMENT_METHODS.tolist()},
        "customer_category_counts": {
            (customer, category): 0
            for customer in CUSTOMER_TYPES.tolist()
            for category in CATEGORIES.tolist()
        },
        "sales_sum": 0.0,
        "sales_min": float("inf"),
        "sales_max": float("-inf"),
        "qty_sum": 0.0,
        "qty_min": float("inf"),
        "qty_max": float("-inf"),
        "invalid_city_region": 0,
        "invalid_category": 0,
        "invalid_payment": 0,
        "null_count": 0,
        "txn_id_sequence_ok": True,
        "last_txn_number": 0,
    }


def _update_validation_state(
    state: Dict[str, Any],
    df: pd.DataFrame,
    transaction_ids: np.ndarray,
    regions: np.ndarray,
    cities: np.ndarray,
    customer_types: np.ndarray,
    categories: np.ndarray,
    payment_methods: np.ndarray,
    sales_amount: np.ndarray,
    quantity: np.ndarray,
) -> None:
    batch_size = int(transaction_ids.shape[0])
    state["total_rows"] += batch_size

    if state["head_df"] is None:
        state["head_df"] = df.head(5).copy()

    _update_counter(state["region_counts"], regions)
    _update_counter(state["customer_counts"], customer_types)
    _update_counter(state["category_counts"], categories)
    _update_counter(state["payment_counts"], payment_methods)

    for customer in CUSTOMER_TYPES.tolist():
        customer_mask = customer_types == customer
        if not np.any(customer_mask):
            continue
        for category in CATEGORIES.tolist():
            state["customer_category_counts"][(customer, category)] += int(
                np.sum(customer_mask & (categories == category))
            )

    state["invalid_city_region"] += _count_invalid_city_region(regions, cities)
    state["invalid_category"] += int((~np.isin(categories, CATEGORIES)).sum())
    state["invalid_payment"] += int((~np.isin(payment_methods, PAYMENT_METHODS)).sum())
    state["null_count"] += int(df.isna().sum().sum())

    state["sales_sum"] += float(np.sum(sales_amount, dtype=np.float64))
    state["sales_min"] = min(state["sales_min"], float(np.min(sales_amount)))
    state["sales_max"] = max(state["sales_max"], float(np.max(sales_amount)))

    state["qty_sum"] += float(np.sum(quantity, dtype=np.float64))
    state["qty_min"] = min(state["qty_min"], float(np.min(quantity)))
    state["qty_max"] = max(state["qty_max"], float(np.max(quantity)))

    first_txn_number = int(transaction_ids[0][4:])
    last_txn_number = int(transaction_ids[-1][4:])
    expected_first = state["last_txn_number"] + 1

    if first_txn_number != expected_first or (last_txn_number - first_txn_number + 1) != batch_size:
        state["txn_id_sequence_ok"] = False

    state["last_txn_number"] = last_txn_number


def _print_distribution(
    title: str,
    counts: Dict[str, int],
    total: int,
    targets: Dict[str, float] | None = None,
) -> None:
    print(f"\n{title}")
    sorted_items = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    for key, count in sorted_items:
        pct = _safe_ratio(count, total) * 100.0
        if targets and key in targets:
            target_pct = targets[key] * 100.0
            delta = pct - target_pct
            print(
                f"- {key}: {count:,} ({pct:.2f}%), "
                f"target={target_pct:.2f}%, delta={delta:+.2f}%"
            )
        else:
            print(f"- {key}: {count:,} ({pct:.2f}%)")


def validate_generated_data(state: Dict[str, Any], expected_rows: int) -> None:
    """Run quick post-generation checks and print summary statistics."""
    total_rows = state["total_rows"]

    print("\n" + "=" * 80)
    print("Quick Validation Report")
    print("=" * 80)
    print(f"Shape: ({total_rows:,}, 9)")

    print("\nHead:")
    if state["head_df"] is not None:
        print(state["head_df"].to_string(index=False))
    else:
        print("No data generated.")

    _print_distribution(
        title="Region distribution",
        counts=state["region_counts"],
        total=total_rows,
        targets=REGION_TARGET_DISTRIBUTION,
    )
    _print_distribution(
        title="Customer type distribution",
        counts=state["customer_counts"],
        total=total_rows,
        targets=CUSTOMER_TYPE_TARGET_DISTRIBUTION,
    )
    _print_distribution(
        title="Category value counts",
        counts=state["category_counts"],
        total=total_rows,
    )
    _print_distribution(
        title="Payment method value counts",
        counts=state["payment_counts"],
        total=total_rows,
    )

    vip_total = sum(state["customer_category_counts"][("VIP", c)] for c in CATEGORIES.tolist())
    normal_total = sum(state["customer_category_counts"][("Normal", c)] for c in CATEGORIES.tolist())

    vip_electronics_ratio = _safe_ratio(
        state["customer_category_counts"][("VIP", "Electronics")], vip_total
    )
    normal_fb_ratio = _safe_ratio(
        state["customer_category_counts"][("Normal", "F&B")], normal_total
    )

    sales_mean = _safe_ratio(state["sales_sum"], total_rows)
    qty_mean = _safe_ratio(state["qty_sum"], total_rows)

    region_close = all(
        abs(_safe_ratio(state["region_counts"][region], total_rows) - target) <= 0.03
        for region, target in REGION_TARGET_DISTRIBUTION.items()
    )
    customer_close = all(
        abs(_safe_ratio(state["customer_counts"][customer], total_rows) - target) <= 0.03
        for customer, target in CUSTOMER_TYPE_TARGET_DISTRIBUTION.items()
    )

    city_region_status = (
        "PASS"
        if state["invalid_city_region"] == 0
        else f"FAIL ({state['invalid_city_region']:,} invalid)"
    )
    category_domain_status = (
        "PASS"
        if state["invalid_category"] == 0
        else f"FAIL ({state['invalid_category']:,} invalid)"
    )
    payment_domain_status = (
        "PASS"
        if state["invalid_payment"] == 0
        else f"FAIL ({state['invalid_payment']:,} invalid)"
    )
    null_status = (
        "PASS"
        if state["null_count"] == 0
        else f"FAIL ({state['null_count']:,} nulls)"
    )

    print("\nCorrelation checks")
    print(f"- VIP -> Electronics ratio: {vip_electronics_ratio * 100:.2f}% (target around 55%)")
    print(f"- Normal -> F&B ratio: {normal_fb_ratio * 100:.2f}% (target around 40%)")

    print("\nMeasure statistics")
    print(
        f"- Sales_Amount: min={state['sales_min']:,.0f}, "
        f"max={state['sales_max']:,.0f}, mean={sales_mean:,.2f}"
    )
    print(
        f"- Quantity: min={state['qty_min']:.0f}, "
        f"max={state['qty_max']:.0f}, mean={qty_mean:.2f}"
    )

    print("\nLogic checks")
    print(f"- Transaction_ID unique sequence: {'PASS' if state['txn_id_sequence_ok'] else 'FAIL'}")
    print(f"- City follows Region mapping: {city_region_status}")
    print(f"- Region distribution close to target: {'PASS' if region_close else 'WARN'}")
    print(f"- Customer_Type distribution close to target: {'PASS' if customer_close else 'WARN'}")
    print(f"- Category domain valid: {category_domain_status}")
    print(f"- Payment domain valid: {payment_domain_status}")
    print(f"- Null values: {null_status}")
    print(f"- Row count check: {'PASS' if total_rows == expected_rows else 'FAIL'}")


def generate_pos_data(
    num_rows: int = DEFAULT_CONFIG["num_rows"],
    seed: int = DEFAULT_CONFIG["seed"],
    output_path: str = DEFAULT_CONFIG["output_path"],
    batch_size: int = DEFAULT_CONFIG["batch_size"],
    progress_every: int = DEFAULT_CONFIG["progress_every"],
    date_start: str = DEFAULT_CONFIG["date_start"],
    date_end: str = DEFAULT_CONFIG["date_end"],
) -> Dict[str, Any]:
    """Generate synthetic POS data and write directly to CSV in batches."""
    if num_rows <= 0:
        raise ValueError("num_rows must be > 0")
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if progress_every <= 0:
        progress_every = batch_size

    if num_rows < 2_000_000 or num_rows > 5_000_000:
        print("[WARN] Recommended range is 2,000,000 to 5,000,000 rows.")

    if os.path.exists(output_path):
        os.remove(output_path)

    rng = np.random.default_rng(seed)
    fake = Faker("vi_VN")
    fake.seed_instance(seed)
    run_id = fake.bothify(text="JOB-#####")

    start_time = datetime.now()
    print("=" * 80)
    print(f"Synthetic POS generation started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Run ID: {run_id}")
    print(f"Rows: {num_rows:,} | Batch size: {batch_size:,} | Seed: {seed}")
    print(f"Date range: {date_start} -> {date_end}")
    print(f"Output path: {os.path.abspath(output_path)}")
    print("=" * 80)

    state = _init_validation_state()
    generated_rows = 0
    next_progress = progress_every

    for start_idx in range(1, num_rows + 1, batch_size):
        current_batch_size = min(batch_size, num_rows - start_idx + 1)

        transaction_ids = generate_transaction_ids(start_index=start_idx, batch_size=current_batch_size)
        dates = generate_dates(
            batch_size=current_batch_size,
            rng=rng,
            start_date=date_start,
            end_date=date_end,
        )
        regions, cities = generate_regions_and_cities(batch_size=current_batch_size, rng=rng)
        customer_types = generate_customer_types(batch_size=current_batch_size, rng=rng)
        categories = generate_categories(
            customer_types=customer_types,
            regions=regions,
            dates=dates,
            rng=rng,
        )
        payment_methods = generate_payment_methods(categories=categories, rng=rng)
        quantity, sales_amount = generate_quantity_and_sales(categories=categories, rng=rng)

        df_batch = build_dataframe(
            transaction_ids=transaction_ids,
            dates=dates,
            regions=regions,
            cities=cities,
            categories=categories,
            customer_types=customer_types,
            payment_methods=payment_methods,
            sales_amount=sales_amount,
            quantity=quantity,
        )

        save_to_csv(
            df=df_batch,
            output_path=output_path,
            mode="a",
            write_header=(start_idx == 1),
        )

        _update_validation_state(
            state=state,
            df=df_batch,
            transaction_ids=transaction_ids,
            regions=regions,
            cities=cities,
            customer_types=customer_types,
            categories=categories,
            payment_methods=payment_methods,
            sales_amount=sales_amount,
            quantity=quantity,
        )

        generated_rows += current_batch_size

        if generated_rows >= next_progress or generated_rows == num_rows:
            elapsed_sec = (datetime.now() - start_time).total_seconds()
            rate = generated_rows / elapsed_sec if elapsed_sec > 0 else 0.0
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"Progress: {generated_rows:,}/{num_rows:,} "
                f"({generated_rows / num_rows * 100:.2f}%) | "
                f"{rate:,.0f} rows/sec"
            )
            while next_progress <= generated_rows:
                next_progress += progress_every

    elapsed_total = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 80)
    print(f"Generation completed in {elapsed_total:.2f} seconds")
    print(f"CSV saved to: {os.path.abspath(output_path)}")
    if os.path.exists(output_path):
        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"Output file size: {file_size_mb:.2f} MB")
    print("=" * 80)

    validate_generated_data(state=state, expected_rows=num_rows)

    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic POS retail data for Iceberg Cube / Star-tree compression."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_CONFIG["num_rows"],
        help="Number of rows to generate (recommended: 2000000-5000000).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_CONFIG["seed"],
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_CONFIG["output_path"],
        help="Output CSV path.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_CONFIG["batch_size"],
        help="Rows per batch when writing CSV.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_CONFIG["progress_every"],
        help="Print progress every N generated rows.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate_pos_data(
        num_rows=args.rows,
        seed=args.seed,
        output_path=args.output,
        batch_size=args.batch_size,
        progress_every=args.progress_every,
    )
