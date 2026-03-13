import pandas as pd
import streamlit as st
import warnings
import mig_functions as mig
import re
import altair as alt


warnings.filterwarnings("ignore")

st.set_page_config(
    layout="wide",
    page_title="MIG Data Processing App",
    page_icon="https://www.agilitypr.com/wp-content/uploads/2025/01/favicon.png"
)

mig.standard_sidebar()

col1, col2 = st.columns([2,3], gap="medium")

with col1:
    st.title("Top Stories")

with col2:
    trend_df = st.session_state.get("filtered_df", pd.DataFrame()).copy()

    if not trend_df.empty and "Date" in trend_df.columns:
        trend_df["Date"] = pd.to_datetime(trend_df["Date"], errors="coerce")
        trend_df = trend_df.dropna(subset=["Date"])
    # trend_df = st.session_state["filtered_df"].copy()
    # trend_df["Date"] = pd.to_datetime(trend_df["Date"], errors="coerce")
    # trend_df = trend_df.dropna(subset=["Date"])

        summary_stats = (
            trend_df.groupby(pd.Grouper(key="Date", freq="D"))
            .agg({"Mentions": "count", "Impressions": "sum"})
            .reset_index()
            .sort_values("Date")
        )

        # Decide whether to show time labels
        show_time = False
        if not summary_stats.empty:
            date_span = summary_stats["Date"].max() - summary_stats["Date"].min()
            show_time = date_span <= pd.Timedelta(days=1)

        x_axis = alt.Axis(
            title=None,
            labelAngle=0,
            format="%b %d, %-I %p" if show_time else "%b %d"
        )

        line = alt.Chart(summary_stats).mark_line(size=2).encode(
            x=alt.X("Date:T", axis=x_axis),
            y=alt.Y("Mentions:Q", axis=None)
        )

        points = alt.Chart(summary_stats).mark_circle(size=55, opacity=0).encode(
            x="Date:T",
            y="Mentions:Q",
            tooltip=[
                alt.Tooltip("Date:T", title="Date", format="%b %d, %Y" if not show_time else "%b %d, %Y %-I:%M %p"),
                alt.Tooltip("Mentions:Q", title="Mentions", format=",")
            ]
        )

        chart = (line + points).properties(height=130)

        st.altair_chart(chart, use_container_width=True)

def normalize_top_stories_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize dataframe enough for Top Stories page to work safely."""
    df = df.copy()

    defaults = {
        "Headline": "",
        "Date": pd.NaT,
        "Mentions": 1,
        "Impressions": 0,
        "Type": "",
        "Outlet": "",
        "URL": "",
        "Snippet": "",
        "Tags": "",
        "Coverage Flags": "",
    }

    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default

    text_columns = ["Headline", "Type", "Outlet", "URL", "Snippet", "Tags", "Coverage Flags"]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    if "Mentions" in df.columns:
        df["Mentions"] = pd.to_numeric(df["Mentions"], errors="coerce").fillna(1).astype(int)

    if "Impressions" in df.columns:
        df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date

    return df


def pick_best_story_details(group: pd.DataFrame):
    """Pick the best example row for a grouped story."""
    if group.empty:
        return None, None, None, None

    working = group.copy()
    working["Outlet"] = working["Outlet"].fillna("").astype(str)
    working["Type"] = working["Type"].fillna("").astype(str)
    working["Snippet"] = working["Snippet"].fillna("").astype(str)
    working["URL"] = working["URL"].fillna("").astype(str)
    working["Impressions"] = pd.to_numeric(working["Impressions"], errors="coerce").fillna(0)

    # First priority: preferred wire services
    preferred_wire_pattern = r"Reuters|Associated Press|Canadian Press"
    preferred_wire_group = working[
        working["Outlet"].str.contains(preferred_wire_pattern, case=False, na=False, regex=True)
    ]

    if not preferred_wire_group.empty:
        best_row = preferred_wire_group.loc[preferred_wire_group["Impressions"].idxmax()]
        return (
            best_row.get("Outlet", None),
            best_row.get("URL", None),
            best_row.get("Type", None),
            best_row.get("Snippet", None),
        )

    # Broadcast gets simpler treatment
    is_broadcast = working["Type"].isin(["TV", "RADIO", "PODCAST"]).any()

    middle_tier_keywords = [
        "MarketWatch", "Seeking Alpha", "News Break", "Dispatchist",
        "MarketScreener", "StreetInsider", "Head Topics"
    ]
    bottom_tier_keywords = [
        "Yahoo", "MSN", "Newswire", "Saltwire", "Market Wire",
        "Business Wire", "TD Ameritrade", "PR Wire", "Chinese Wire",
        "News Wire", "Presswire"
    ]

    middle_pattern = "|".join(re.escape(x) for x in middle_tier_keywords)
    bottom_pattern = "|".join(re.escape(x) for x in bottom_tier_keywords)
    combined_pattern = "|".join(re.escape(x) for x in (middle_tier_keywords + bottom_tier_keywords))

    if is_broadcast:
        best_row = working.loc[working["Impressions"].idxmax()]
        return (
            best_row.get("Outlet", None),
            best_row.get("URL", None),
            best_row.get("Type", None),
            best_row.get("Snippet", None),
        )

    top_tier_group = working[
        ~working["Outlet"].str.contains(combined_pattern, case=False, na=False, regex=True)
    ]
    middle_tier_group = working[
        working["Outlet"].str.contains(middle_pattern, case=False, na=False, regex=True)
        & ~working["Outlet"].str.contains(bottom_pattern, case=False, na=False, regex=True)
    ]

    if not top_tier_group.empty:
        best_row = top_tier_group.loc[top_tier_group["Impressions"].idxmax()]
    elif not middle_tier_group.empty:
        best_row = middle_tier_group.loc[middle_tier_group["Impressions"].idxmax()]
    else:
        best_row = working.loc[working["Impressions"].idxmax()]

    return (
        best_row.get("Outlet", None),
        best_row.get("URL", None),
        best_row.get("Type", None),
        best_row.get("Snippet", None),
    )


@st.cache_data
def group_and_process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Group stories by headline/date and attach best example row details."""
    df = normalize_top_stories_df(df)

    rows = []
    for (headline, date_value), group in df.groupby(["Headline", "Date"], dropna=False):
        mentions = pd.to_numeric(group["Mentions"], errors="coerce").fillna(0).sum()
        impressions = pd.to_numeric(group["Impressions"], errors="coerce").fillna(0).sum()

        best_outlet, best_url, best_type, best_snippet = pick_best_story_details(group)

        rows.append({
            "Headline": headline,
            "Date": date_value,
            "Mentions": int(mentions),
            "Impressions": impressions,
            "Example Outlet": best_outlet,
            "Example URL": best_url,
            "Example Type": best_type,
            "Example Snippet": best_snippet,
        })

    if not rows:
        return pd.DataFrame(columns=[
            "Headline", "Date", "Mentions", "Impressions",
            "Example Outlet", "Example URL", "Example Type", "Example Snippet"
        ])

    return pd.DataFrame(rows)


def tokenize_boolean_query(query: str):
    """
    Tokenize boolean query into:
    - quoted phrases
    - parentheses
    - AND / OR / NOT
    - bare words
    """
    token_pattern = r'"[^"]+"|\(|\)|\bAND\b|\bOR\b|\bNOT\b|[^\s()]+'
    tokens = re.findall(token_pattern, query, flags=re.IGNORECASE)
    return [token.strip() for token in tokens if token.strip()]


def normalize_tokens_for_implicit_and(tokens):
    """
    Insert implicit AND where appropriate, e.g.:
    term term -> term AND term
    ) term -> ) AND term
    term ( -> term AND (
    ) ( -> ) AND (
    """
    normalized = []

    def is_operand(tok):
        upper = tok.upper()
        return tok not in ("(", ")") and upper not in ("AND", "OR", "NOT")

    def ends_expr(tok):
        return tok == ")" or is_operand(tok)

    def starts_expr(tok):
        upper = tok.upper()
        return tok == "(" or is_operand(tok) or upper == "NOT"

    for i, token in enumerate(tokens):
        if i > 0:
            prev = tokens[i - 1]
            if ends_expr(prev) and starts_expr(token):
                normalized.append("AND")
        normalized.append(token)

    return normalized


def boolean_query_to_postfix(tokens):
    """
    Convert infix boolean tokens to postfix using shunting-yard algorithm.
    Precedence: NOT > AND > OR
    NOT is treated as unary operator.
    """
    precedence = {"NOT": 3, "AND": 2, "OR": 1}
    right_associative = {"NOT"}
    output = []
    operators = []

    for token in tokens:
        upper = token.upper()

        if upper in precedence:
            while (
                operators
                and operators[-1] != "("
                and (
                    precedence.get(operators[-1], 0) > precedence[upper]
                    or (
                        precedence.get(operators[-1], 0) == precedence[upper]
                        and upper not in right_associative
                    )
                )
            ):
                output.append(operators.pop())
            operators.append(upper)

        elif token == "(":
            operators.append(token)

        elif token == ")":
            while operators and operators[-1] != "(":
                output.append(operators.pop())
            if operators and operators[-1] == "(":
                operators.pop()

        else:
            output.append(token)

    while operators:
        output.append(operators.pop())

    return output


def evaluate_boolean_query(series: pd.Series, query: str) -> pd.Series:
    """
    Evaluate a boolean query against a pandas Series.
    Supports:
    - quoted phrases
    - parentheses
    - AND / OR / NOT
    Case-insensitive
    """
    series = series.fillna("").astype(str)

    query = str(query).strip()
    if not query:
        return pd.Series(True, index=series.index)

    tokens = tokenize_boolean_query(query)
    if not tokens:
        return pd.Series(True, index=series.index)

    tokens = normalize_tokens_for_implicit_and(tokens)
    postfix = boolean_query_to_postfix(tokens)
    stack = []

    for token in postfix:
        upper = token.upper()

        if upper == "NOT":
            if len(stack) < 1:
                return pd.Series(False, index=series.index)
            operand = stack.pop()
            stack.append(~operand)

        elif upper == "AND":
            if len(stack) < 2:
                return pd.Series(False, index=series.index)
            right = stack.pop()
            left = stack.pop()
            stack.append(left & right)

        elif upper == "OR":
            if len(stack) < 2:
                return pd.Series(False, index=series.index)
            right = stack.pop()
            left = stack.pop()
            stack.append(left | right)

        else:
            term = token[1:-1] if len(token) >= 2 and token.startswith('"') and token.endswith('"') else token
            mask = series.str.contains(term, case=False, na=False, regex=False)
            stack.append(mask)

    if len(stack) != 1:
        return pd.Series(False, index=series.index)

    return stack[0]


def apply_filters(
    df: pd.DataFrame,
    start_date,
    end_date,
    exclude_types,
    exclude_coverage_flags,
    advanced_filters,
):
    """Apply all filters to the source dataframe before grouping."""
    working = df.copy()
    working = normalize_top_stories_df(working)

    if start_date is not None:
        working = working[working["Date"].notna()]
        working = working[working["Date"] >= start_date]

    if end_date is not None:
        working = working[working["Date"].notna()]
        working = working[working["Date"] <= end_date]

    if exclude_types:
        working = working[~working["Type"].isin(exclude_types)]

    if exclude_coverage_flags and "Coverage Flags" in working.columns:
        working = working[~working["Coverage Flags"].isin(exclude_coverage_flags)]

    for condition in advanced_filters:
        column = condition.get("column")
        value = condition.get("value")

        if not column or not value or column not in working.columns:
            continue

        series = working[column].fillna("").astype(str)
        mask = evaluate_boolean_query(series, value)
        working = working[mask]

    return working


def save_selected_rows(updated_data: pd.DataFrame, key: str):
    """Persist selected top stories into session state."""
    if "Top Story" not in updated_data.columns:
        return

    selected_rows = updated_data.loc[updated_data["Top Story"] == True].copy()
    selected_rows.drop(columns=["Top Story"], inplace=True, errors="ignore")
    st.session_state.selected_rows = selected_rows

    if st.button("Save Selected", key=key, type="primary"):
        st.session_state.added_df = pd.concat(
            [st.session_state.added_df, selected_rows],
            ignore_index=True
        )

        if not st.session_state.added_df.empty:
            st.session_state.added_df.drop_duplicates(
                subset=["Headline", "Date"],
                keep="last",
                inplace=True
            )
            st.session_state.added_df.reset_index(drop=True, inplace=True)

        st.rerun()


def reset_generated_candidates():
    st.session_state.df_grouped = pd.DataFrame()
    st.session_state.filtered_df = pd.DataFrame()


# Initialize session state variables
df_vars = ["filtered_df", "df_grouped", "selected_df", "selected_rows", "top_stories", "added_df"]
for var in df_vars:
    if var not in st.session_state:
        st.session_state[var] = pd.DataFrame()

if "top_stories_generated" not in st.session_state:
    st.session_state.top_stories_generated = False

if not st.session_state.added_df.empty:
    st.session_state.added_df = normalize_top_stories_df(st.session_state.added_df)

if not st.session_state.get("upload_step", False):
    st.error("Please upload a CSV before trying this step.")
elif not st.session_state.get("standard_step", False):
    st.error("Please run the Standard Cleaning before trying this step.")
else:
    source_df = normalize_top_stories_df(st.session_state.df_traditional.copy())

    # Keep columns needed for this page plus tag/prominence fields if present
    all_columns = list(source_df.columns)
    columns_to_keep = [
        "Headline", "Date", "Mentions", "Impressions", "Type",
        "Outlet", "URL", "Snippet", "Tags", "Coverage Flags"
    ]

    extra_filter_columns = [
        col for col in all_columns
        if col.lower().startswith("tag")
        or "tag group" in col.lower()
        or "prominence" in col.lower()
    ]
    columns_to_keep.extend(extra_filter_columns)

    existing_columns = [col for col in columns_to_keep if col in source_df.columns]
    source_df = source_df[existing_columns].copy()
    source_df = normalize_top_stories_df(source_df)

    available_types = sorted([t for t in source_df["Type"].dropna().astype(str).unique().tolist() if t])
    available_flags = sorted([f for f in source_df["Coverage Flags"].dropna().astype(str).unique().tolist() if f])

    advanced_filter_columns = []

    for col in source_df.columns:
        col_lower = col.lower()

        if (
            col in ["Headline", "Outlet", "Coverage Flags", "Tags", "Language", "Country"]
            or col_lower.startswith("tag group:")
            or col_lower.startswith("tag ")
            or "tag group" in col_lower
            or "prominence" in col_lower
        ):
            advanced_filter_columns.append(col)

    advanced_filter_columns = list(dict.fromkeys(advanced_filter_columns))
    advanced_filter_columns.sort()

    min_available_date = source_df["Date"].min() if source_df["Date"].notna().any() else None
    max_available_date = source_df["Date"].max() if source_df["Date"].notna().any() else None

    with st.form(key="top_stories_filter_form"):
        filter_col1, filter_col2, filter_col3 = st.columns([2.5, 2, 4], gap="medium")

        with filter_col1:
            if min_available_date and max_available_date:
                date_range = st.date_input(
                    "Date range",
                    value=(min_available_date, max_available_date),
                    min_value=min_available_date,
                    max_value=max_available_date,
                )
            else:
                date_range = ()

        with filter_col2:
            exclude_types = st.multiselect(
                "Exclude media types",
                options=available_types,
                default=[]
            )

        with filter_col3:
            exclude_coverage_flags = st.multiselect(
                "Exclude coverage flags",
                options=available_flags,
                default=[f for f in available_flags if f in [
                    "Newswire?", "Market Report Spam?", "Stocks / Financials?", "Advertorial?", "Aggregator"
                ]]
            )

        with st.expander("Advanced filters", expanded=False):
            adv1_col1, adv1_col2 = st.columns([2, 5], gap="small")
            with adv1_col1:
                adv_col_1 = st.selectbox("Condition 1 column", options=[""] + advanced_filter_columns, index=0)
            with adv1_col2:
                adv_value_1 = st.text_input(
                    "Condition 1 query",
                    help='Use boolean syntax like: ("Class action" OR lawsuit) AND NOT "dismissed"'
                )

            adv2_col1, adv2_col2 = st.columns([2, 5], gap="small")
            with adv2_col1:
                adv_col_2 = st.selectbox("Condition 2 column", options=[""] + advanced_filter_columns, index=0)
            with adv2_col2:
                adv_value_2 = st.text_input(
                    "Condition 2 query",
                    help='Use boolean syntax like: (High OR Moderate) AND NOT Low'
                )

            adv3_col1, adv3_col2 = st.columns([2, 5], gap="small")
            with adv3_col1:
                adv_col_3 = st.selectbox("Condition 3 column", options=[""] + advanced_filter_columns, index=0)
            with adv3_col2:
                adv_value_3 = st.text_input(
                    "Condition 3 query",
                    help='Use boolean syntax like: Canada OR USA'
                )

        action_col1, action_col2 = st.columns([2, 1], gap="small")
        with action_col1:
            generate_candidates = st.form_submit_button("Generate Possible Top Stories", type="primary")
        with action_col2:
            clear_generated = st.form_submit_button("Clear Generated Results")

    if clear_generated:
        reset_generated_candidates()
        st.session_state.top_stories_generated = False
        st.rerun()

    if generate_candidates:
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date, end_date = None, None

        advanced_filters = [
            {"column": adv_col_1, "value": adv_value_1},
            {"column": adv_col_2, "value": adv_value_2},
            {"column": adv_col_3, "value": adv_value_3},
        ]

        filtered_df = apply_filters(
            df=source_df,
            start_date=start_date,
            end_date=end_date,
            exclude_types=exclude_types,
            exclude_coverage_flags=exclude_coverage_flags,
            advanced_filters=advanced_filters,
        )

        st.session_state.filtered_df = filtered_df.copy()
        st.session_state.df_grouped = group_and_process_data(filtered_df)
        st.session_state.df_grouped = st.session_state.df_grouped.sort_values(
            by=["Mentions", "Impressions"],
            ascending=[False, False]
        )
        st.session_state.top_stories_generated = True

    # Results area
    if st.session_state.top_stories_generated:
        filtered_count = len(st.session_state.filtered_df) if not st.session_state.filtered_df.empty else 0
        grouped_count = len(st.session_state.df_grouped) if not st.session_state.df_grouped.empty else 0

        st.caption(f"{filtered_count:,} mentions matched filters, producing {grouped_count:,} grouped story candidates.")

        df_to_display = st.session_state.df_grouped.copy()

        if not df_to_display.empty:
            df_to_display["Date"] = pd.to_datetime(df_to_display["Date"], errors="coerce").dt.date
            df_to_display["Example URL"] = df_to_display["Example URL"].fillna("").astype(str)
            df_to_display["Impressions"] = pd.to_numeric(df_to_display["Impressions"], errors="coerce").fillna(0).astype(int)
            df_to_display = df_to_display.sort_values(
                by=["Mentions", "Impressions"],
                ascending=[False, False]
            )

        # Remove already-saved rows from candidate list
        if not st.session_state.added_df.empty and not df_to_display.empty:
            existing_pairs = set(
                zip(st.session_state.added_df["Headline"], st.session_state.added_df["Date"])
            )
            df_to_display = df_to_display[
                ~df_to_display.apply(
                    lambda row: (row["Headline"], row["Date"]) in existing_pairs,
                    axis=1
                )
            ].copy()

        if df_to_display.empty:
            st.info("No story candidates matched the selected filters.")
        else:
            df_to_display["Top Story"] = False

            st.subheader(
                "Possible Top Stories",
                help='Check the "Top Story" box for those stories you want to select, then click "Save Selected" below.'
            )

            updated_data_custom = st.data_editor(
                df_to_display,
                key="df_by_custom",
                use_container_width=True,
                column_config={
                    "Headline": st.column_config.Column("Headline", width="large"),
                    "Date": st.column_config.Column("Date", width="small"),
                    "Mentions": st.column_config.Column("Mentions", width="small"),
                    "Impressions": st.column_config.NumberColumn("Impressions", width="small", format="%,d"),
                    "Example URL": st.column_config.LinkColumn("Example URL", width="medium"),
                    "Example Snippet": st.column_config.Column("Example Snippet", width="small"),
                    "Example Outlet": None,
                    "Example Type": None,
                    "Top Story": st.column_config.Column("Top Story", width="small"),
                },
                hide_index=True,
            )

            save_selected_rows(updated_data_custom, key="by_custom")

    # Saved top stories section
    if len(st.session_state.added_df) > 0:
        st.subheader("Saved Top Stories")

        saved_df = st.session_state.added_df.copy()
        saved_df = normalize_top_stories_df(saved_df)
        saved_df = saved_df.sort_values(by="Date", ascending=True).reset_index(drop=True)
        saved_df["Impressions"] = pd.to_numeric(saved_df["Impressions"], errors="coerce").fillna(0).astype(int)

        saved_columns = ["Headline", "Date", "Mentions", "Impressions", "Example URL"]
        existing_saved_columns = [col for col in saved_columns if col in saved_df.columns]
        saved_df = saved_df[existing_saved_columns].copy()

        saved_df["Delete"] = False

        if "Date" in saved_df.columns:
            date_column = saved_df.pop("Date")
            saved_df.insert(1, "Date", date_column)

        updated_data = st.data_editor(
            saved_df,
            use_container_width=True,
            column_config={
                "Delete": st.column_config.CheckboxColumn("Delete", width="small"),
                "Headline": st.column_config.Column("Headline", width="large"),
                "Date": st.column_config.Column("Date", width="small"),
                "Mentions": st.column_config.Column("Mentions", width="small"),
                "Impressions": st.column_config.NumberColumn("Impressions", width="small", format="%,d"),
                "Example URL": st.column_config.LinkColumn("Example URL", width="medium"),
            },
            hide_index=True,
            key="saved_stories_editor"
        )

        rows_to_delete = updated_data[updated_data["Delete"]].index.tolist()

        if rows_to_delete:
            cleaned_saved_df = updated_data.drop(index=rows_to_delete).copy()
            cleaned_saved_df.drop(columns=["Delete"], inplace=True, errors="ignore")
            st.session_state.added_df = cleaned_saved_df.reset_index(drop=True)
            st.rerun()

        if st.button("Clear Saved"):
            st.session_state.added_df = pd.DataFrame(
                columns=["Headline", "Date", "Mentions", "Impressions", "Example URL"]
            )
            st.rerun()