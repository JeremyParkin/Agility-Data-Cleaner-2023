import streamlit as st
import pandas as pd
import mig_functions as mig
import warnings
import urllib.parse
import numpy as np

warnings.filterwarnings("ignore")

st.set_page_config(
    layout="wide",
    page_title="MIG Data Processing App",
    page_icon="https://www.agilitypr.com/wp-content/uploads/2025/01/favicon.png"
)

mig.standard_sidebar()

st.title("Authors - Missing")
mig.require_standard_pipeline()

if len(st.session_state.get("df_traditional", [])) == 0:
    st.subheader("No traditional media in data. Skip to next step.")
    st.stop()


# ---------- Session state init ----------

if "last_author_fix" not in st.session_state:
    st.session_state.last_author_fix = None

if "auth_reviewed_count" not in st.session_state:
    st.session_state.auth_reviewed_count = 0


# ---------- Helpers ----------

def fixable_headline_stats(df, primary="Headline", secondary="Author"):
    """Return stats on how many author fields can be fixed."""
    total = df["Mentions"].count()

    if total == 0:
        return {
            "total": 0,
            "total_known": 0,
            "percent_known": "0%",
            "fixable": 0,
            "fixable_headline_count": 0,
            "remaining": 0,
            "percent_knowable": "0%"
        }

    headline_stats = pd.pivot_table(
        df,
        index=primary,
        values=["Mentions", secondary],
        aggfunc="count"
    )

    headline_stats["Missing"] = headline_stats["Mentions"] - headline_stats[secondary]
    missing = headline_stats["Missing"].sum()

    headline_stats = headline_stats[headline_stats[secondary] > 0]
    headline_stats = headline_stats[headline_stats["Missing"] > 0]

    fixable = headline_stats["Missing"].sum()
    fixable_headline_count = headline_stats["Missing"].count()

    counter = st.session_state.auth_skip_counter
    remaining = max(fixable_headline_count - counter, 0)

    total_known = total - missing
    percent_known = "{:.0%}".format(total_known / total) if total > 0 else "0%"
    percent_knowable = "{:.0%}".format((total - (missing - fixable)) / total) if total > 0 else "0%"

    return {
        "total": total,
        "total_known": total_known,
        "percent_known": percent_known,
        "fixable": fixable,
        "fixable_headline_count": fixable_headline_count,
        "remaining": remaining,
        "percent_knowable": percent_knowable
    }


def undo_last_author_fix():
    """
    Undo the most recent author fix applied on this page.
    Restores only the previously affected rows' Author values.
    """
    last_fix = st.session_state.get("last_author_fix")

    if not last_fix:
        return

    row_indexes = last_fix.get("row_indexes", [])
    previous_authors = last_fix.get("previous_authors")
    previous_reviewed_count = last_fix.get("previous_reviewed_count", 0)

    if not row_indexes or previous_authors is None:
        st.session_state.last_author_fix = None
        return

    valid_row_indexes = [idx for idx in row_indexes if idx in st.session_state.df_traditional.index]

    if len(valid_row_indexes) == len(previous_authors):
        st.session_state.df_traditional.loc[valid_row_indexes, "Author"] = previous_authors.values

    st.session_state.auth_reviewed_count = previous_reviewed_count
    st.session_state.last_author_fix = None


# ---------- Page setup ----------

counter = st.session_state.auth_skip_counter
reviewed = st.session_state.auth_reviewed_count

hide_table_row_index = """
    <style>
    tbody th {display:none}
    .blank {display:none}
    </style>
"""
st.markdown(hide_table_row_index, unsafe_allow_html=True)

author_working_df = st.session_state.df_traditional.copy()
author_working_df["Author"] = author_working_df["Author"].replace("", np.nan)

available_flags = []
if "Coverage Flags" in author_working_df.columns:
    available_flags = sorted([
        f for f in author_working_df["Coverage Flags"].fillna("").astype(str).unique().tolist()
        if f.strip()
    ])

default_excluded_flags = [
    f for f in [
        "Newswire?",
        "Market Report Spam?",
        "Stocks / Financials?",
        "Advertorial?",
    ] if f in available_flags
]

HIDDEN_FLAGS = {"Good Outlet", "Aggregator"}
visible_flags = [f for f in available_flags if f not in HIDDEN_FLAGS]
visible_defaults = [f for f in default_excluded_flags if f not in HIDDEN_FLAGS]

excluded_flags = st.multiselect(
    "Exclude coverage flags",
    options=visible_flags,
    default=visible_defaults,
    help="Exclude selected flagged coverage from the missing-author workflow on this page."
)

if excluded_flags and "Coverage Flags" in author_working_df.columns:
    author_working_df = author_working_df[
        ~author_working_df["Coverage Flags"].fillna("").isin(excluded_flags)
    ].copy()

headline_table = author_working_df[["Headline", "Mentions", "Author"]].copy()
headline_table = headline_table.groupby("Headline").count()
headline_table["Missing"] = headline_table["Mentions"] - headline_table["Author"]
headline_table = headline_table[
    (headline_table["Author"] > 0) & (headline_table["Missing"] > 0)
].sort_values("Missing", ascending=False).reset_index()
headline_table.rename(
    columns={"Author": "Known", "Mentions": "Total"},
    inplace=True,
    errors="raise"
)

temp_headline_list = headline_table.copy()


# ---------- Main workflow ----------

if len(temp_headline_list) == 0:
    st.success("No fixable missing-author headlines remain in the current filtered view.")

elif counter < len(temp_headline_list):
    headline_text = temp_headline_list.iloc[counter]["Headline"]
    encoded_headline = urllib.parse.quote(f'"{headline_text}"')
    google_search_url = f"https://www.google.com/search?q={encoded_headline}"

    headline_authors_df = mig.headline_authors(author_working_df, headline_text).copy()

    if len(headline_authors_df) > 0:
        if "Author" in headline_authors_df.columns:
            possibles = headline_authors_df["Author"].dropna().astype(str).tolist()
        elif "Matches" in headline_authors_df.columns:
            possibles = headline_authors_df["Matches"].dropna().astype(str).tolist()
        else:
            possibles = []
    else:
        possibles = []

    if not possibles:
        possibles = [""]

    but1, col3, but2, but4 = st.columns(4)

    with but1:
        next_auth = st.button("Skip to Next Headline")
        if next_auth:
            st.session_state.auth_skip_counter = counter + 1
            st.rerun()

    with col3:
        if counter > 0:
            st.write(f"Skipped: {counter}")

    with but2:
        if counter > 0:
            reset_counter = st.button("Reset Skip Counter")
            if reset_counter:
                st.session_state.auth_skip_counter = 0
                st.rerun()

    with but4:
        undo_available = st.session_state.get("last_author_fix") is not None
        undo_clicked = st.button(
            "Undo Last Author Update",
            disabled=not undo_available,
            help="Reverses the most recent author update applied on this page."
        )
        if undo_clicked:
            undo_last_author_fix()
            st.rerun()

    form_block = st.container()
    info_block = st.container()

    with info_block:
        col1, col2, col3 = st.columns([12, 1, 9])

        with col1:
            st.subheader("Headline")
            st.table(headline_table.iloc[[counter]])
            st.markdown(
                f'&nbsp;&nbsp;» <a href="{google_search_url}" target="_blank" style="text-decoration:underline; color:lightblue;">Search Google for this headline</a>',
                unsafe_allow_html=True
            )

        with col2:
            st.write(" ")

        with col3:
            st.subheader("Authors in CSV")
            authors_display = headline_authors_df.copy()

            if "Author" in authors_display.columns:
                authors_display = authors_display.rename(columns={"Author": "Possible Author(s)"})
            elif "Matches" in authors_display.columns:
                authors_display = authors_display.rename(columns={"Matches": "Possible Author(s)"})

            st.table(authors_display)

    with form_block:
        with st.form("auth updater", clear_on_submit=True):
            col1, col2, col3 = st.columns([8, 1, 8])

            with col1:
                box_author = st.selectbox(
                    "Pick from possible Authors",
                    possibles,
                    help="Pick from one of the authors already associated with this headline."
                )

            with col2:
                st.write(" ")
                st.subheader("OR")

            with col3:
                string_author = st.text_input(
                    "Write in the author name",
                    help="Override above selection by writing in a custom name."
                )

            submitted = st.form_submit_button("Update Author", type="primary")

        if submitted:
            new_author = string_author.strip() if len(string_author.strip()) > 0 else box_author

            if not new_author:
                st.warning("Please choose or enter an author name.")
            else:
                matching_rows = st.session_state.df_traditional.index[
                    st.session_state.df_traditional["Headline"] == headline_text
                ].tolist()

                st.session_state.last_author_fix = {
                    "headline": headline_text,
                    "row_indexes": matching_rows,
                    "previous_authors": st.session_state.df_traditional.loc[matching_rows, "Author"].copy(),
                    "previous_reviewed_count": st.session_state.auth_reviewed_count,
                }

                mig.fix_author(st.session_state.df_traditional, headline_text, new_author)
                st.session_state.auth_reviewed_count = reviewed + 1
                st.rerun()

else:
    st.info("You've reached the end of the list!")

    top_end_col1, top_end_col2 = st.columns([1, 1])

    with top_end_col1:
        if counter > 0:
            reset_counter = st.button("Reset Counter")
            if reset_counter:
                st.session_state.auth_skip_counter = 0
                st.rerun()

    with top_end_col2:
        undo_available = st.session_state.get("last_author_fix") is not None
        undo_clicked = st.button(
            "Undo Last Author Update",
            disabled=not undo_available,
            help="Reverses the most recent author update applied on this page."
        )
        if undo_clicked:
            undo_last_author_fix()
            st.rerun()

    if counter == 0:
        st.success("✓ Nothing left to update here.")


# ---------- Bottom stats ----------

st.divider()

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Original Top Authors")
    media_type_column = "Type" if "Type" in st.session_state.df_untouched.columns else "Media Type"

    filtered_df = st.session_state.df_untouched[
        st.session_state.df_untouched[media_type_column].isin(
            ["PRINT", "ONLINE_NEWS", "ONLINE", "BLOGS", "PRESS_RELEASE"]
        )
    ].copy()

    if "Mentions" not in filtered_df.columns:
        filtered_df["Mentions"] = 1

    original_top_authors = mig.top_x_by_mentions(filtered_df, "Author")
    st.write(original_top_authors)

with col2:
    st.subheader("New Top Authors")
    st.dataframe(mig.top_x_by_mentions(author_working_df, "Author"))

with col3:
    st.subheader("Fixable Author Stats")
    remaining = fixable_headline_stats(author_working_df, primary="Headline", secondary="Author")

    statscol1, statscol2 = st.columns(2)

    with statscol1:
        reviewed_display = (
            len(temp_headline_list) - remaining["remaining"] + reviewed
            if len(temp_headline_list) > 0 else reviewed
        )
        st.metric(label="Reviewed", value=reviewed_display)
        st.metric(label="Updated", value=reviewed)

    with statscol2:
        st.metric(label="Remaining in this view", value=remaining["remaining"])