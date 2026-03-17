from __future__ import annotations

import sys
from itertools import combinations
from math import ceil
from pathlib import Path

import geopandas as gpd
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.config import CRS_CANADA_ALBERS, DATA_INTERMEDIATE  # noqa: E402


TOP_DA_SHARE = 0.10
TOP_REGION_COUNT = 5
SIMILAR_DA_SPEARMAN_THRESHOLD = 0.95
SIMILAR_DA_TOP_OVERLAP_THRESHOLD = 0.70
SIMILAR_REGION_SPEARMAN_THRESHOLD = 0.90


FORMULAS = {
    "current_multiplicative": {
        "label": "E * (S - A)",
        "fn": lambda e, s, a: e * (s - a),
    },
    "additive_protective": {
        "label": "(E + S + (1 - A)) / 3",
        "fn": lambda e, s, a: (e + s + (1 - a)) / 3.0,
    },
    "hybrid_exposure_weighted": {
        "label": "E * ((S + (1 - A)) / 2)",
        "fn": lambda e, s, a: e * ((s + (1 - a)) / 2.0),
    },
}


def normalize_01(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn = s.min(skipna=True)
    mx = s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series([pd.NA] * len(s), index=s.index, dtype="Float64")
    return (s - mn) / (mx - mn)


def top_overlap(
    df: pd.DataFrame,
    score_a: str,
    score_b: str,
    top_n: int,
    id_col: str,
) -> dict[str, float]:
    top_a = set(df.nlargest(top_n, score_a)[id_col].tolist())
    top_b = set(df.nlargest(top_n, score_b)[id_col].tolist())
    overlap = len(top_a & top_b)
    union = len(top_a | top_b)
    return {
        "top_n": float(top_n),
        "overlap_count": float(overlap),
        "overlap_rate": float(overlap / top_n) if top_n else 0.0,
        "jaccard": float(overlap / union) if union else 0.0,
    }


def pairwise_formula_metrics(
    df: pd.DataFrame,
    formula_names: list[str],
    raw_suffix: str,
    rank_top_n: int,
    id_col: str,
) -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    for left, right in combinations(formula_names, 2):
        raw_left = f"{left}{raw_suffix}"
        raw_right = f"{right}{raw_suffix}"
        spearman = df[[raw_left, raw_right]].corr(method="spearman").iloc[0, 1]
        overlap = top_overlap(df, raw_left, raw_right, rank_top_n, id_col=id_col)
        rows.append(
            {
                "formula_a": left,
                "formula_b": right,
                "spearman": float(spearman),
                "top_n": int(overlap["top_n"]),
                "overlap_count": int(overlap["overlap_count"]),
                "overlap_rate": float(overlap["overlap_rate"]),
                "jaccard": float(overlap["jaccard"]),
            }
        )
    return pd.DataFrame(rows)


def choose_recommendation(
    da_metrics: pd.DataFrame,
    region_metrics: pd.DataFrame,
    formula_summary: pd.DataFrame,
) -> tuple[str, str]:
    summary = formula_summary.set_index("formula")
    current_negative_share = float(summary.loc["current_multiplicative", "negative_share"])

    additive_vs_hybrid_da = da_metrics.loc[
        (da_metrics["formula_a"] == "additive_protective")
        & (da_metrics["formula_b"] == "hybrid_exposure_weighted")
    ].iloc[0]
    additive_vs_hybrid_region = region_metrics.loc[
        (region_metrics["formula_a"] == "additive_protective")
        & (region_metrics["formula_b"] == "hybrid_exposure_weighted")
    ].iloc[0]

    if (
        current_negative_share > 0.50
        and float(additive_vs_hybrid_da["spearman"]) >= SIMILAR_DA_SPEARMAN_THRESHOLD
        and float(additive_vs_hybrid_da["overlap_rate"]) >= SIMILAR_DA_TOP_OVERLAP_THRESHOLD
        and float(additive_vs_hybrid_region["spearman"]) >= SIMILAR_REGION_SPEARMAN_THRESHOLD
    ):
        return (
            "additive_protective",
            "Additive and hybrid formulas produce very similar high-risk geography, while the current multiplicative formula remains mostly negative in raw form. Because interpretability is the priority, the additive protective formula is preferred.",
        )

    current_vs_additive = da_metrics.loc[
        (da_metrics["formula_a"] == "current_multiplicative")
        & (da_metrics["formula_b"] == "additive_protective")
    ].iloc[0]
    current_vs_hybrid = da_metrics.loc[
        (da_metrics["formula_a"] == "current_multiplicative")
        & (da_metrics["formula_b"] == "hybrid_exposure_weighted")
    ].iloc[0]

    hybrid_is_more_continuous = (
        float(current_vs_hybrid["spearman"]) >= float(current_vs_additive["spearman"]) + 0.05
        or float(current_vs_hybrid["overlap_rate"]) >= float(current_vs_additive["overlap_rate"]) + 0.10
    )

    if current_negative_share > 0.50 and hybrid_is_more_continuous:
        return (
            "hybrid_exposure_weighted",
            "The current multiplicative formula is difficult to interpret because most raw scores are negative. The hybrid formula avoids that issue and preserves more of the current ranking pattern than the additive alternative.",
        )

    if current_negative_share <= 0.10:
        return (
            "current_multiplicative",
            "The current multiplicative formula does not show a large negative-value problem and remains viable. No alternative demonstrates a strong enough advantage to justify changing it.",
        )

    return (
        "additive_protective",
        "The additive protective formula is the simplest interpretable option and avoids the mostly negative raw-score behavior of the current multiplicative formula.",
    )


def main() -> int:
    comp_csv = DATA_INTERMEDIATE / "hvi_da_components.csv"
    da_gpkg = DATA_INTERMEDIATE / "da.gpkg"

    if not comp_csv.exists():
        print(f"ERROR: Missing {comp_csv}. Run scripts/05_build_hvi_outputs.py first.")
        return 1
    if not da_gpkg.exists():
        print(f"ERROR: Missing {da_gpkg}. Run scripts/01_prepare_da.py first.")
        return 1

    project_root = Path(__file__).resolve().parents[1]
    admin_path = (
        project_root
        / "data_raw"
        / "Administrative_Boundaries_-6445306865161621642"
        / "Administrative_Boundaries.shp"
    )
    if not admin_path.exists():
        print(
            "ERROR: Metro admin boundary shapefile not found.\n"
            f"Expected at: {admin_path}",
            file=sys.stderr,
        )
        return 1

    print("=== 06_formula_review.py ===")
    print("DA components:", comp_csv)
    print("DA geometry:", da_gpkg)
    print("Admin boundaries:", admin_path)

    df = pd.read_csv(comp_csv, low_memory=False)
    required_cols = {"DGUID", "pop_total", "exposure_index", "sensitivity_index", "adaptive_capacity_index"}
    missing = required_cols - set(df.columns)
    if missing:
        print(f"ERROR: hvi_da_components.csv missing required columns: {sorted(missing)}")
        return 1

    df["DGUID"] = df["DGUID"].astype(str)
    for col in ["pop_total", "exposure_index", "sensitivity_index", "adaptive_capacity_index"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    compare_df = df.dropna(subset=["exposure_index", "sensitivity_index", "adaptive_capacity_index"]).copy()
    compare_df = compare_df.rename(
        columns={
            "exposure_index": "E",
            "sensitivity_index": "S",
            "adaptive_capacity_index": "A",
        }
    )
    print(f"Complete DAs used for formula comparison: {len(compare_df):,}")

    formula_names = list(FORMULAS.keys())
    for name, spec in FORMULAS.items():
        raw_col = f"{name}_raw"
        n01_col = f"{name}_n01"
        compare_df[raw_col] = spec["fn"](
            compare_df["E"].astype(float),
            compare_df["S"].astype(float),
            compare_df["A"].astype(float),
        )
        compare_df[n01_col] = normalize_01(compare_df[raw_col])

    da_out_cols = ["DGUID", "pop_total", "E", "S", "A"]
    for name in formula_names:
        da_out_cols.extend([f"{name}_raw", f"{name}_n01"])
    da_out = compare_df[da_out_cols].copy()

    da_out_csv = DATA_INTERMEDIATE / "hvi_formula_comparison_da.csv"
    da_out.to_csv(da_out_csv, index=False)
    print("Wrote:", da_out_csv)

    formula_summary_rows: list[dict[str, float | str]] = []
    for name, spec in FORMULAS.items():
        raw_col = f"{name}_raw"
        n01_col = f"{name}_n01"
        raw = pd.to_numeric(compare_df[raw_col], errors="coerce")
        n01 = pd.to_numeric(compare_df[n01_col], errors="coerce")
        formula_summary_rows.append(
            {
                "formula": name,
                "label": spec["label"],
                "count": int(raw.notna().sum()),
                "negative_share": float((raw < 0).mean()),
                "raw_mean": float(raw.mean()),
                "raw_std": float(raw.std()),
                "raw_min": float(raw.min()),
                "raw_median": float(raw.median()),
                "raw_max": float(raw.max()),
                "n01_mean": float(n01.mean()),
                "n01_std": float(n01.std()),
            }
        )
    formula_summary = pd.DataFrame(formula_summary_rows)

    top_da_n = max(1, ceil(TOP_DA_SHARE * len(compare_df)))
    da_metrics = pairwise_formula_metrics(compare_df, formula_names, "_n01", top_da_n, id_col="DGUID")

    da_geo = gpd.read_file(da_gpkg, layer="da")
    if "DGUID" not in da_geo.columns:
        print("ERROR: DA layer missing DGUID.")
        return 1
    da_geo["DGUID"] = da_geo["DGUID"].astype(str)
    da_geo = da_geo[da_geo["DGUID"].isin(compare_df["DGUID"])].copy()
    da_geo = da_geo[["DGUID", "geometry"]].merge(da_out[["DGUID", "pop_total"]], on="DGUID", how="left")
    da_geo = da_geo.to_crs(CRS_CANADA_ALBERS)

    admin = gpd.read_file(admin_path)
    if admin.empty:
        print("ERROR: admin boundaries layer is empty.")
        return 1
    keep_admin_fields = [c for c in ["FullName", "ShortName", "MunNum"] if c in admin.columns]
    if "FullName" not in keep_admin_fields:
        print("ERROR: admin boundaries missing FullName field.")
        print("Columns:", list(admin.columns))
        return 1
    admin = admin[keep_admin_fields + ["geometry"]].copy().to_crs(CRS_CANADA_ALBERS)

    inter = gpd.overlay(da_geo[["DGUID", "geometry"]], admin, how="intersection", keep_geom_type=True)
    if inter.empty:
        print("ERROR: DA/admin overlay produced no intersections.")
        return 1

    inter["inter_area_m2"] = inter.geometry.area
    inter = inter.sort_values(["DGUID", "inter_area_m2"], ascending=[True, False])
    da_to_region = inter.drop_duplicates(subset=["DGUID"], keep="first").copy()

    region_join_cols = ["DGUID"] + keep_admin_fields
    region_src = da_out.merge(da_to_region[region_join_cols], on="DGUID", how="left")
    region_src["pop_total"] = pd.to_numeric(region_src["pop_total"], errors="coerce")
    region_src = region_src.dropna(subset=["FullName", "pop_total"]).copy()
    region_src = region_src[region_src["pop_total"] > 0].copy()

    agg_map: dict[str, tuple[str, str]] = {
        "region_pop_total": ("pop_total", "sum"),
        "da_count_used": ("DGUID", "nunique"),
    }
    for name in formula_names:
        weighted_col = f"{name}_weighted"
        region_src[weighted_col] = region_src["pop_total"].astype(float) * pd.to_numeric(
            region_src[f"{name}_raw"], errors="coerce"
        )
        agg_map[f"{name}_weighted_sum"] = (weighted_col, "sum")

    region_stats = region_src.groupby(keep_admin_fields, dropna=False).agg(**agg_map).reset_index()
    for name in formula_names:
        weighted_sum_col = f"{name}_weighted_sum"
        raw_pw_col = f"{name}_raw_pw"
        n01_col = f"{name}_n01"
        region_stats[raw_pw_col] = region_stats[weighted_sum_col] / region_stats["region_pop_total"]
        region_stats[n01_col] = normalize_01(region_stats[raw_pw_col])
    weighted_sum_cols = [f"{name}_weighted_sum" for name in formula_names]
    region_stats = region_stats.drop(columns=weighted_sum_cols)

    region_out_csv = DATA_INTERMEDIATE / "hvi_formula_comparison_regions.csv"
    region_stats.to_csv(region_out_csv, index=False)
    print("Wrote:", region_out_csv)

    top_region_n = min(TOP_REGION_COUNT, len(region_stats))
    region_stats["region_id"] = region_stats["FullName"].astype(str)
    region_metrics = pairwise_formula_metrics(
        region_stats,
        formula_names,
        "_n01",
        top_region_n,
        id_col="region_id",
    )

    recommended_formula, recommendation_reason = choose_recommendation(
        da_metrics=da_metrics,
        region_metrics=region_metrics,
        formula_summary=formula_summary,
    )

    report_path = DATA_INTERMEDIATE / "06_formula_review_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("06_formula_review report\n\n")
        f.write(f"DA components input: {comp_csv}\n")
        f.write(f"DA geometry input: {da_gpkg}\n")
        f.write(f"Admin boundary input: {admin_path}\n\n")

        f.write("Formula definitions:\n")
        for name, spec in FORMULAS.items():
            f.write(f"  {name}: {spec['label']}\n")
        f.write("\n")

        f.write(f"DA count used for comparison: {len(compare_df):,}\n")
        f.write(f"DA top-risk overlap threshold: top {top_da_n:,} DAs ({TOP_DA_SHARE:.0%})\n")
        f.write(f"Region count used for comparison: {len(region_stats):,}\n")
        f.write(f"Region top-risk overlap threshold: top {top_region_n:,} regions\n\n")

        f.write("Decision rule:\n")
        f.write(
            "  Interpretability is the priority. If additive and hybrid formulas produce similar high-risk geography,\n"
            "  prefer the additive protective formula. Keep the current multiplicative formula only if it avoids the\n"
            "  negative-score problem and shows a clear analytical advantage.\n\n"
        )

        f.write("Formula summaries:\n")
        f.write(formula_summary.to_string(index=False))
        f.write("\n\n")

        f.write("DA rank comparison (pairwise):\n")
        f.write(da_metrics.to_string(index=False))
        f.write("\n\n")

        f.write("Region rank comparison (pairwise):\n")
        f.write(region_metrics.to_string(index=False))
        f.write("\n\n")

        f.write("Top regions by formula:\n")
        for name in formula_names:
            f.write(f"\n{name}:\n")
            top_regions = region_stats.nlargest(top_region_n, f"{name}_n01")[keep_admin_fields + [f"{name}_n01"]]
            f.write(top_regions.to_string(index=False))
            f.write("\n")

        f.write("\nRecommendation:\n")
        f.write(f"  Recommended formula: {recommended_formula}\n")
        f.write(f"  Why: {recommendation_reason}\n")

    print("Wrote:", report_path)
    print("Recommended formula:", recommended_formula)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
