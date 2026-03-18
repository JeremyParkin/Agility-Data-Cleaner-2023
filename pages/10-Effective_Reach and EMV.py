import math
import warnings
import numpy as np
import pandas as pd
import streamlit as st
import mig_functions as mig

warnings.filterwarnings("ignore")

st.set_page_config(
    layout="wide",
    page_title="Effective Reach + EMV",
    page_icon="https://www.agilitypr.com/wp-content/uploads/2025/01/favicon.png"
)

mig.standard_sidebar()
st.title("Effective Reach and EMV")
mig.require_standard_pipeline()


# =========================================================
# Defaults
# =========================================================
HIDDEN_UI_PARAMS = {
    "ONLINE_DENOMINATOR_COEFF",
    "ONLINE_DENOMINATOR_EXPONENT",
}

DEFAULT_TRAD_MEDIA_PARAMS = {
    "print_generic": {
        "MID_IMPRESSIONS": 65_000,
        "HIGH_IMPRESSIONS": 1_500_000,
        "BENCHMARK_VIS": 0.38,
        "A_SIZE": 0.30,
        "A_MIN": 0.20,
        "A_MAX": 0.40,
        "LOW_IMPRESSIONS": 0,
        "GATE_ANCHOR": 150_000,
        "MIN_VISIBILITY": 0.09,
        "MAX_VISIBILITY": 0.30,
        "CPM": 35,
    },
    "print_magazine": {
        "MID_IMPRESSIONS": 75_000,
        "HIGH_IMPRESSIONS": 1_000_000,
        "BENCHMARK_VIS": 0.45,
        "A_SIZE": 0.30,
        "A_MIN": 0.20,
        "A_MAX": 0.40,
        "LOW_IMPRESSIONS": 0,
        "GATE_ANCHOR": 150_000,
        "MIN_VISIBILITY": 0.10,
        "MAX_VISIBILITY": 0.70,
        "CPM": 40,
    },
    "print_daily": {
        "MID_IMPRESSIONS": 50_000,
        "HIGH_IMPRESSIONS": 2_000_000,
        "BENCHMARK_VIS": 0.23,
        "A_SIZE": 0.30,
        "A_MIN": 0.20,
        "A_MAX": 0.40,
        "LOW_IMPRESSIONS": 0,
        "GATE_ANCHOR": 50_000,
        "MIN_VISIBILITY": 0.05,
        "MAX_VISIBILITY": 0.40,
        "CPM": 25,
    },
    "tv": {
        "MID_IMPRESSIONS": 500_000,
        "HIGH_IMPRESSIONS": 1_000_000,
        "BENCHMARK_VIS": 0.488,
        "A_SIZE": 0.85,
        "A_MIN": 0.05,
        "A_MAX": 0.30,
        "LOW_IMPRESSIONS": 0,
        "GATE_ANCHOR": 750_000,
        "MIN_VISIBILITY": 0.15,
        "MAX_VISIBILITY": 0.65,
        "CPM": 35,
    },
    "radio": {
        "MID_IMPRESSIONS": 100_000,
        "HIGH_IMPRESSIONS": 500_000,
        "BENCHMARK_VIS": 0.375,
        "A_SIZE": 0.25,
        "A_MIN": 0.05,
        "A_MAX": 0.30,
        "LOW_IMPRESSIONS": 0,
        "GATE_ANCHOR": 150_000,
        "MIN_VISIBILITY": 0.10,
        "MAX_VISIBILITY": 0.55,
        "CPM": 15,
    },
    "online": {
        "CPM": 15,
        "DAILY_VISITOR_RATE": 0.30,
        "PAGES_PER_VISIT": 2.0,
        "ONLINE_DENOMINATOR_COEFF": 2.3746,
        "ONLINE_DENOMINATOR_EXPONENT": 0.2009,
    }
}

DEFAULT_PLATFORM_PARAMS = {
    "x": {
        "MID_FOLLOWERS": 70_500,
        "HIGH_FOLLOWERS": 5_000_000,
        "BENCHMARK_VIS": 0.015,
        "EXPECTED_ENG_RATE": 0.0012,
        "PERF_EXPONENT": 0.50,
        "PERF_FLOOR": 0.60,
        "PERF_CAP": 1.80,
        "GATE_FOLLOWERS_ANCHOR": 200_000,
        "MAX_VISIBILITY": 0.20,
        "CPM": 6,
    },
    "bluesky": {
        "MID_FOLLOWERS": 15_000,
        "HIGH_FOLLOWERS": 500_000,
        "BENCHMARK_VIS": 0.018,
        "EXPECTED_ENG_RATE": 0.0012,
        "PERF_EXPONENT": 0.50,
        "PERF_FLOOR": 0.60,
        "PERF_CAP": 1.80,
        "GATE_FOLLOWERS_ANCHOR": 40_000,
        "MAX_VISIBILITY": 0.22,
        "CPM": 6,
    },
    "instagram": {
        "MID_FOLLOWERS": 497_900,
        "HIGH_FOLLOWERS": 10_000_000,
        "BENCHMARK_VIS": 0.076,
        "EXPECTED_ENG_RATE": 0.0048,
        "PERF_EXPONENT": 0.50,
        "PERF_FLOOR": 0.60,
        "PERF_CAP": 1.80,
        "GATE_FOLLOWERS_ANCHOR": 1_100_000,
        "MAX_VISIBILITY": 0.20,
        "CPM": 7,
    },
    "facebook": {
        "MID_FOLLOWERS": 346_300,
        "HIGH_FOLLOWERS": 10_000_000,
        "BENCHMARK_VIS": 0.043,
        "EXPECTED_ENG_RATE": 0.0015,
        "PERF_EXPONENT": 0.50,
        "PERF_FLOOR": 0.60,
        "PERF_CAP": 1.80,
        "GATE_FOLLOWERS_ANCHOR": 850_000,
        "MAX_VISIBILITY": 0.15,
        "CPM": 7,
    },
    "linkedin": {
        "MID_FOLLOWERS": 26_500,
        "HIGH_FOLLOWERS": 1_000_000,
        "BENCHMARK_VIS": 0.121,
        "EXPECTED_ENG_RATE": 0.0060,
        "PERF_EXPONENT": 0.50,
        "PERF_FLOOR": 0.60,
        "PERF_CAP": 1.90,
        "GATE_FOLLOWERS_ANCHOR": 50_000,
        "MAX_VISIBILITY": 0.40,
        "CPM": 7,
    },
    "tiktok": {
        "MID_FOLLOWERS": 46_900,
        "HIGH_FOLLOWERS": 10_000_000,
        "BENCHMARK_VIS": 0.25,
        "EXPECTED_ENG_RATE": 0.037,
        "PERF_EXPONENT": 0.60,
        "PERF_FLOOR": 0.55,
        "PERF_CAP": 2.50,
        "GATE_FOLLOWERS_ANCHOR": 110_000,
        "MAX_VISIBILITY": 2.0,
        "CPM": 5,
    },
    "youtube": {
        "MID_FOLLOWERS": 68_800,
        "HIGH_FOLLOWERS": 10_000_000,
        "BENCHMARK_VIS": 0.12,
        "EXPECTED_ENG_RATE": 0.0030,
        "PERF_EXPONENT": 0.60,
        "PERF_FLOOR": 0.55,
        "PERF_CAP": 2.50,
        "GATE_FOLLOWERS_ANCHOR": 150_000,
        "MAX_VISIBILITY": 5.0,
        "CPM": 11,
    },
    "reddit": {
        "MID_FOLLOWERS": 25_000,
        "HIGH_FOLLOWERS": 2_000_000,
        "BENCHMARK_VIS": 0.08,
        "EXPECTED_ENG_RATE": 0.01,
        "PERF_EXPONENT": 0.70,
        "PERF_FLOOR": 0.50,
        "PERF_CAP": 2.50,
        "GATE_FOLLOWERS_ANCHOR": 75_000,
        "MAX_VISIBILITY": 0.60,
        "CPM": 4,
    },
}

PARAM_HELP = {
    "MID_IMPRESSIONS": "Benchmark audience size where the baseline visibility assumption is centered.",
    "HIGH_IMPRESSIONS": "Upper reference point used in size scaling for traditional media.",
    "BENCHMARK_VIS": "Baseline visibility rate before additional adjustments.",
    "A_SIZE": "Controls how strongly visibility changes as audience size changes.",
    "A_MIN": "Lower bound on the size-response factor.",
    "A_MAX": "Upper bound on the size-response factor.",
    "LOW_IMPRESSIONS": "Reserved lower impression threshold parameter.",
    "GATE_ANCHOR": "Point where large-audience dampening begins.",
    "MIN_VISIBILITY": "Minimum allowed final visibility.",
    "MAX_VISIBILITY": "Maximum allowed final visibility.",
    "CPM": "Cost per thousand used to calculate EMV.",
    "DAILY_VISITOR_RATE": "Estimated share of monthly unique visitors who visit daily.",
    "PAGES_PER_VISIT": "Average number of pages viewed per visit.",
    "ONLINE_DENOMINATOR_COEFF": "Calibration coefficient used in the online effective reach formula.",
    "ONLINE_DENOMINATOR_EXPONENT": "Calibration exponent used in the online effective reach formula.",
    "MID_FOLLOWERS": "Benchmark follower size where the social baseline visibility assumption is centered.",
    "HIGH_FOLLOWERS": "Upper reference point used in social size scaling.",
    "EXPECTED_ENG_RATE": "Expected engagement rate used as the baseline for performance adjustment.",
    "PERF_EXPONENT": "Controls how strongly over- or under-performance affects visibility.",
    "PERF_FLOOR": "Lower bound on the performance multiplier.",
    "PERF_CAP": "Upper bound on the performance multiplier.",
    "GATE_FOLLOWERS_ANCHOR": "Point where large-account dampening begins.",
}

SECTION_EXPLAINER = """
**Traditional / print / broadcast**
- **MID_IMPRESSIONS**: benchmark audience size where the baseline visibility assumption is centered  
- **HIGH_IMPRESSIONS**: upper reference point for size scaling  
- **BENCHMARK_VIS**: baseline visibility rate before additional adjustments  
- **A_SIZE**: controls how strongly visibility changes as audience size changes  
- **A_MIN / A_MAX**: lower and upper bounds on that size-response factor  
- **GATE_ANCHOR**: point where large-audience dampening begins  
- **MIN_VISIBILITY / MAX_VISIBILITY**: floor and ceiling for final visibility  
- **CPM**: cost per thousand used for EMV  

**Online**
- **DAILY_VISITOR_RATE**: share of monthly unique visitors who visit daily  
- **PAGES_PER_VISIT**: average number of pages viewed per visit  
- **ONLINE_DENOMINATOR_COEFF / EXPONENT**: calibration parameters in the online effective reach formula  
- **CPM**: cost per thousand used for EMV  

**Social**
- **MID_FOLLOWERS / HIGH_FOLLOWERS**: benchmark audience sizes used for scaling  
- **BENCHMARK_VIS**: baseline visibility before engagement/performance adjustments  
- **EXPECTED_ENG_RATE**: expected engagement rate used as the comparison baseline  
- **PERF_EXPONENT**: controls how strongly over/under-performance affects visibility  
- **PERF_FLOOR / PERF_CAP**: lower and upper bounds on that performance multiplier  
- **GATE_FOLLOWERS_ANCHOR**: point where large-account dampening begins  
- **MAX_VISIBILITY**: ceiling on final visibility  
- **CPM**: cost per thousand used for EMV  
"""

DISPLAY_FORMAT_DICT = {
    "AVE": "${:,.0f}",
    "Mentions": "{:,.0f}",
    "Engagements": "{:,.0f}",
    "Impressions": "{:,.0f}",
    "Effective Reach": "{:,.0f}",
    "EMV": "${:,.2f}",
}


# =========================================================
# Session state
# =========================================================

if "er_trad_params" not in st.session_state:
    st.session_state.er_trad_params = {
        k: v.copy() for k, v in DEFAULT_TRAD_MEDIA_PARAMS.items()
    }

if "er_platform_params" not in st.session_state:
    st.session_state.er_platform_params = {
        k: v.copy() for k, v in DEFAULT_PLATFORM_PARAMS.items()
    }


def restore_er_defaults():
    st.session_state.er_trad_params = {
        k: v.copy() for k, v in DEFAULT_TRAD_MEDIA_PARAMS.items()
    }
    st.session_state.er_platform_params = {
        k: v.copy() for k, v in DEFAULT_PLATFORM_PARAMS.items()
    }


# =========================================================
# Helpers
# =========================================================

DEFAULT_EPS = 1e-12


def clamp(x, lo, hi):
    return min(max(x, lo), hi)


def safe_ln1p(x: float) -> float:
    return math.log1p(max(0.0, float(x)))


def get_param_input_settings(param_key, value):
    value = float(value)
    abs_value = abs(value)

    float_params = {
        "BENCHMARK_VIS",
        "A_SIZE",
        "A_MIN",
        "A_MAX",
        "MIN_VISIBILITY",
        "MAX_VISIBILITY",
        "DAILY_VISITOR_RATE",
        "PAGES_PER_VISIT",
        "ONLINE_DENOMINATOR_COEFF",
        "ONLINE_DENOMINATOR_EXPONENT",
        "EXPECTED_ENG_RATE",
        "PERF_EXPONENT",
        "PERF_FLOOR",
        "PERF_CAP",
    }

    if param_key in float_params:
        if abs_value < 0.1:
            return {"step": 0.01, "format": "%.4f"}
        if abs_value < 1:
            return {"step": 0.01, "format": "%.2f"}
        if abs_value < 10:
            return {"step": 0.1, "format": "%.2f"}
        return {"step": 0.1, "format": "%.2f"}

    if abs_value < 10:
        return {"step": 1.0, "format": "%.0f"}
    if abs_value < 100:
        return {"step": 1.0, "format": "%.0f"}
    if abs_value < 1000:
        return {"step": 10.0, "format": "%.0f"}
    if abs_value < 10000:
        return {"step": 100.0, "format": "%.0f"}
    if abs_value < 100000:
        return {"step": 1000.0, "format": "%.0f"}

    return {"step": 10000.0, "format": "%.0f"}


def normalize_platform(value):
    if pd.isna(value):
        return None

    s = str(value).strip().lower()
    aliases = {
        "twitter": "x",
        "x.com": "x",
        "x (twitter)": "x",
        "ig": "instagram",
        "insta": "instagram",
        "fb": "facebook",
        "meta": "facebook",
        "li": "linkedin",
        "ln": "linkedin",
        "tt": "tiktok",
        "tik tok": "tiktok",
        "yt": "youtube",
        "you tube": "youtube",
    }
    return aliases.get(s, s)


def normalize_trad_media_type(media_type: str) -> str | None:
    s = str(media_type).strip().lower()
    mapping = {
        "online": "online",
        "print": "print_generic",
        "tv": "tv",
        "radio": "radio",
    }
    return mapping.get(s)


def trad_size_percentile_ln(impressions: float, high_i: float) -> float:
    num = safe_ln1p(impressions)
    den = safe_ln1p(high_i)
    if den <= 0:
        return 0.0
    return clamp(num / den, 0.0, 1.0)


def compute_trad_vis_size(impressions: float, p: dict) -> float:
    mid_i = float(p["MID_IMPRESSIONS"])
    high_i = float(p["HIGH_IMPRESSIONS"])
    base_vis = float(p["BENCHMARK_VIS"])

    p_size = trad_size_percentile_ln(impressions, high_i)
    p0 = trad_size_percentile_ln(mid_i, high_i)
    dist = abs(p_size - p0)

    a = float(p["A_SIZE"])
    a_min = float(p["A_MIN"])
    a_max = float(p["A_MAX"])
    a_used = clamp(a * (1.0 + dist), a_min, a_max)

    m = math.exp(-a_used * (p_size - p0))
    return base_vis * m


def compute_trad_gate(impressions: float, p: dict) -> float:
    anchor = float(p["GATE_ANCHOR"])
    impressions = max(float(impressions), 0.0)

    if impressions <= anchor:
        return 1.0

    gate = 1.0 / (1.0 + safe_ln1p(impressions / anchor - 1.0))
    return clamp(gate, 0.55, 1.0)


def compute_online_single(impressions: float, p: dict):
    impressions = float(impressions)

    if impressions <= 0:
        effective_reach = 0
    else:
        daily_visitor_rate = float(p["DAILY_VISITOR_RATE"])
        pages_per_visit = float(p["PAGES_PER_VISIT"])
        coeff = float(p["ONLINE_DENOMINATOR_COEFF"])
        exponent = float(p["ONLINE_DENOMINATOR_EXPONENT"])

        eff_reach_raw = (
            impressions * daily_visitor_rate * pages_per_visit
        ) / (coeff * (impressions ** exponent))

        effective_reach = int(round(eff_reach_raw))

    cpm = float(p.get("CPM", 0.0))
    emv = round((effective_reach / 1000.0) * cpm, 2)
    return effective_reach, emv


def compute_trad_single(media_type, impressions, trad_params):
    key = normalize_trad_media_type(media_type)

    if key is None:
        return None, None

    if key == "online":
        return compute_online_single(impressions, trad_params["online"])

    if key not in trad_params:
        return None, None

    p = trad_params[key]
    impressions = float(impressions)

    if impressions <= 0:
        return 0, 0.0

    vis_size = compute_trad_vis_size(impressions, p)
    gate = compute_trad_gate(impressions, p)

    vis_final = clamp(
        vis_size * gate,
        float(p["MIN_VISIBILITY"]),
        float(p["MAX_VISIBILITY"])
    )

    effective_reach = int(round(impressions * vis_final))
    emv = round((effective_reach / 1000.0) * float(p.get("CPM", 0.0)), 2)
    return effective_reach, emv


def compute_vis_size(followers, p):
    f = max(float(followers), 1.0)
    mid = float(p["MID_FOLLOWERS"])
    high = float(p["HIGH_FOLLOWERS"])
    bench = float(p["BENCHMARK_VIS"])
    max_vis = float(p["MAX_VISIBILITY"])

    z = (np.log10(f) - np.log10(mid)) / (np.log10(high) - np.log10(mid) + DEFAULT_EPS)
    z = clamp(float(z), 0.0, 1.0)

    small_boost = 1.35
    large_floor = 0.92

    vis = bench * ((1 - z) * small_boost + z * large_floor)
    return clamp(float(vis), 0.0, max_vis)


def compute_perf_index(followers, engagements, p):
    f = max(float(followers), 1.0)
    e = max(float(engagements), 0.0)
    eng_rate = e / (f + DEFAULT_EPS)

    expected = float(p["EXPECTED_ENG_RATE"])
    perf_ratio = eng_rate / (expected + DEFAULT_EPS)
    raw = perf_ratio ** float(p["PERF_EXPONENT"])

    return clamp(raw, float(p["PERF_FLOOR"]), float(p["PERF_CAP"]))


def compute_gate(followers, p):
    f = max(float(followers), 1.0)
    anchor = float(p["GATE_FOLLOWERS_ANCHOR"])
    high = float(p["HIGH_FOLLOWERS"])

    z = (np.log10(f) - np.log10(anchor)) / (np.log10(high) - np.log10(anchor) + DEFAULT_EPS)
    z = clamp(float(z), 0.0, 1.0)

    damp = 1.0 - 0.35 * z
    return clamp(damp, 0.50, 1.00)


def compute_ser_single(platform, followers, engagements, platform_params):
    key = normalize_platform(platform)

    if key is None or key not in platform_params:
        return None, None

    p = platform_params[key]
    followers = float(followers)
    engagements = float(engagements)

    if followers <= 0:
        return 0, 0.0

    vis_size = compute_vis_size(followers, p)
    perf_index = compute_perf_index(followers, engagements, p)
    gate = compute_gate(followers, p)

    vis_final = clamp(
        vis_size * perf_index * gate,
        0.0,
        float(p["MAX_VISIBILITY"])
    )

    effective_reach_raw = followers * vis_final
    effective_reach = int(round(effective_reach_raw))
    emv = round((effective_reach_raw / 1000.0) * float(p.get("CPM", 0.0)), 2)

    return effective_reach, emv


def apply_traditional_metrics(df: pd.DataFrame, trad_params: dict) -> pd.DataFrame:
    df = df.copy()

    if "Type" not in df.columns or "Impressions" not in df.columns:
        df["Effective Reach"] = np.nan
        df["EMV"] = np.nan
        return df

    df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)

    effective_reach = []
    emv = []

    for _, row in df.iterrows():
        er, emv_val = compute_trad_single(
            media_type=row.get("Type", ""),
            impressions=row.get("Impressions", 0),
            trad_params=trad_params
        )
        effective_reach.append(er if er is not None else np.nan)
        emv.append(emv_val if emv_val is not None else np.nan)

    df["Effective Reach"] = effective_reach
    df["EMV"] = emv
    return df


def apply_social_metrics(df: pd.DataFrame, platform_params: dict) -> pd.DataFrame:
    df = df.copy()

    platform_col = "Type" if "Type" in df.columns else None
    followers_col = "Impressions" if "Impressions" in df.columns else None
    engagements_col = "Engagements" if "Engagements" in df.columns else None

    if not platform_col or not followers_col or not engagements_col:
        df["Effective Reach"] = np.nan
        df["EMV"] = np.nan
        return df

    df[followers_col] = pd.to_numeric(df[followers_col], errors="coerce").fillna(0)
    df[engagements_col] = pd.to_numeric(df[engagements_col], errors="coerce").fillna(0)

    effective_reach = []
    emv = []

    for _, row in df.iterrows():
        er, emv_val = compute_ser_single(
            platform=row.get(platform_col, ""),
            followers=row.get(followers_col, 0),
            engagements=row.get(engagements_col, 0),
            platform_params=platform_params
        )
        effective_reach.append(er if er is not None else np.nan)
        emv.append(emv_val if emv_val is not None else np.nan)

    df["Effective Reach"] = effective_reach
    df["EMV"] = emv
    return df


def build_type_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Type" not in df.columns:
        return pd.DataFrame()

    agg_dict = {}

    if "Mentions" in df.columns:
        agg_dict["Mentions"] = ("Mentions", "sum")
    if "Impressions" in df.columns:
        agg_dict["Impressions"] = ("Impressions", "sum")
    if "Effective Reach" in df.columns:
        agg_dict["Effective Reach"] = ("Effective Reach", "sum")
    if "EMV" in df.columns:
        agg_dict["EMV"] = ("EMV", "sum")

    if not agg_dict:
        return pd.DataFrame()

    sort_candidates = [c for c in ["Effective Reach", "Impressions", "Mentions"] if c in agg_dict]

    breakdown = (
        df.groupby("Type", dropna=False)
        .agg(**agg_dict)
        .reset_index()
    )

    if sort_candidates:
        breakdown = breakdown.sort_values(by=sort_candidates, ascending=False)

    return breakdown


def render_metric_expander(df: pd.DataFrame, title: str, preview_rows: int = 50):
    if df is None or len(df) == 0:
        return

    with st.expander(title):
        col1, col1b, col2 = st.columns([1,1,2])

        with col1:
            st.metric(label="Mentions", value="{:,}".format(len(df)))

            if "Effective Reach" in df.columns:
                er_total = pd.to_numeric(df["Effective Reach"], errors="coerce").fillna(0).sum()
                st.metric(
                    label="Effective Reach",
                    value=mig.format_number(er_total),
                    help=f"{er_total:,.0f}"
                )



        with col1b:
            if "Impressions" in df.columns:
                impressions_total = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0).sum()
                st.metric(
                    label="Impressions",
                    value=mig.format_number(impressions_total),
                    help=f"{impressions_total:,.0f}"
                )



            if "EMV" in df.columns:
                emv_total = pd.to_numeric(df["EMV"], errors="coerce").fillna(0).sum()
                st.metric(
                    label="EMV",
                    value=f"{mig.format_number(emv_total)}",
                    help=f"{emv_total:,.2f}"
                )

        with col2:
            st.subheader("Media Type")
            breakdown = build_type_breakdown(df)

            if not breakdown.empty:
                st.dataframe(
                    breakdown.style.format(DISPLAY_FORMAT_DICT, na_rep=""),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No type breakdown available.")

        st.subheader("Data")
        st.caption(f"(First {min(len(df), preview_rows):,} rows)")

        preview_df = df.head(preview_rows).copy()
        cell_count = preview_df.shape[0] * preview_df.shape[1]

        if cell_count <= 262144:
            st.dataframe(
                preview_df.style.format(DISPLAY_FORMAT_DICT, na_rep=""),
                use_container_width=True
            )
        else:
            st.dataframe(
                preview_df.fillna(""),
                use_container_width=True
            )


# =========================================================
# UI
# =========================================================

with st.expander("Parameters", expanded=False):
    top_a, top_b = st.columns([1, 4])

    with top_a:
        if st.button("Restore defaults"):
            restore_er_defaults()
            st.rerun()

    with top_b:
        st.caption("These parameters reset to their defaults at the start of each new session.")

    with st.expander("What these parameters mean", expanded=False):
        st.markdown(SECTION_EXPLAINER)

    st.markdown("### Traditional / online")
    for media_key, params in st.session_state.er_trad_params.items():
        with st.expander(media_key.replace("_", " ").title(), expanded=False):
            cols = st.columns(3)
            visible_items = [
                (param_key, param_value)
                for param_key, param_value in params.items()
                if param_key not in HIDDEN_UI_PARAMS
            ]

            for i, (param_key, param_value) in enumerate(visible_items):
                settings = get_param_input_settings(param_key, param_value)
                help_text = PARAM_HELP.get(param_key, "")

                with cols[i % 3]:
                    st.session_state.er_trad_params[media_key][param_key] = st.number_input(
                        param_key,
                        value=float(param_value),
                        step=float(settings["step"]),
                        format=settings["format"],
                        help=help_text,
                        key=f"trad_{media_key}_{param_key}"
                    )


    st.markdown("### Social")
    for platform_key, params in st.session_state.er_platform_params.items():
        with st.expander(platform_key.replace("_", " ").title(), expanded=False):
            cols = st.columns(3)
            for i, (param_key, param_value) in enumerate(params.items()):
                settings = get_param_input_settings(param_key, param_value)
                help_text = PARAM_HELP.get(param_key, "")

                with cols[i % 3]:
                    st.session_state.er_platform_params[platform_key][param_key] = st.number_input(
                        param_key,
                        value=float(param_value),
                        step=float(settings["step"]),
                        format=settings["format"],
                        help=help_text,
                        key=f"social_{platform_key}_{param_key}"
                    )

run_metrics = st.button("Calculate Effective Reach + EMV", type="primary")

if run_metrics:
    st.session_state.df_traditional = apply_traditional_metrics(
        st.session_state.df_traditional,
        st.session_state.er_trad_params
    )

    st.session_state.df_social = apply_social_metrics(
        st.session_state.df_social,
        st.session_state.er_platform_params
    )

    trad_calculated = (
        st.session_state.df_traditional["Effective Reach"].notna().sum()
        if "Effective Reach" in st.session_state.df_traditional.columns else 0
    )
    social_calculated = (
        st.session_state.df_social["Effective Reach"].notna().sum()
        if "Effective Reach" in st.session_state.df_social.columns else 0
    )

    st.success("Effective Reach and EMV added to current session data.")
    st.caption(
        f"Traditional rows calculated: {trad_calculated:,} · Social rows calculated: {social_calculated:,}"
    )

st.divider()

show_results = (
    ("Effective Reach" in st.session_state.df_traditional.columns)
    or ("Effective Reach" in st.session_state.df_social.columns)
)

if show_results:
    render_metric_expander(st.session_state.df_traditional, "Traditional", preview_rows=50)
    render_metric_expander(st.session_state.df_social, "Social", preview_rows=50)