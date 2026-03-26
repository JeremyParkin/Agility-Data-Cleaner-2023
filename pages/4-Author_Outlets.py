import urllib.parse
import warnings

import numpy as np
import pandas as pd
import requests
import streamlit as st
from requests.structures import CaseInsensitiveDict
from unidecode import unidecode

import mig_functions as mig

warnings.filterwarnings("ignore")

st.set_page_config(
    layout="wide",
    page_title="MIG Data Processing App",
    page_icon="https://www.agilitypr.com/wp-content/uploads/2025/01/favicon.png"
)

mig.standard_sidebar()
st.title("Author - Outlets")
mig.require_standard_pipeline()


format_dict = {
    'AVE': '${0:,.0f}',
    'Audience Reach': '{:,d}',
    'Impressions': '{:,d}'
}

if "last_outlet_assignment" not in st.session_state:
    st.session_state.last_outlet_assignment = None


def undo_last_outlet_assignment():
    last_assignment = st.session_state.get("last_outlet_assignment")

    if not last_assignment:
        return

    author_name = last_assignment.get("author_name")
    previous_outlet = last_assignment.get("previous_outlet", "")
    previous_skip = last_assignment.get("previous_skip", st.session_state.auth_outlet_skipped)

    if not author_name:
        st.session_state.last_outlet_assignment = None
        return

    st.session_state.auth_outlet_table = st.session_state.auth_outlet_table.copy()
    st.session_state.auth_outlet_table.loc[
        st.session_state.auth_outlet_table["Author"] == author_name,
        "Outlet"
    ] = previous_outlet

    st.session_state.auth_outlet_skipped = previous_skip
    st.session_state.last_outlet_assignment = None


def reset_skips():
    st.session_state.auth_outlet_skipped = 0


def fetch_outlet(author_name: str):
    contact_url = "https://mediadatabase.agilitypr.com/api/v4/contacts/search"

    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "text/json"
    headers["Accept"] = "text/json"
    headers["Authorization"] = st.secrets["authorization"]
    headers["client_id"] = st.secrets["client_id"]
    headers["userclient_id"] = st.secrets["userclient_id"]

    data = f"""
    {{
      "aliases": [
        "{author_name}"
      ]
    }}
    """

    contact_resp = requests.post(contact_url, headers=headers, data=data)
    return contact_resp.json()

def build_auth_outlet_table(df: pd.DataFrame, top_auths_by: str,
                            existing_assignments: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Rebuild the author-outlet summary table from df_traditional and preserve
    any existing outlet assignments where possible.
    """
    working = df[["Author", "Mentions", "Impressions"]].copy()

    # Normalize and exclude blank authors
    working["Author"] = working["Author"].fillna("").astype(str).str.strip()
    working = working[working["Author"] != ""].copy()

    rebuilt = (
        working.groupby("Author", as_index=False)[["Mentions", "Impressions"]]
        .sum()
    )

    if existing_assignments is not None and len(
            existing_assignments) > 0 and "Outlet" in existing_assignments.columns:
        assignment_map = (
            existing_assignments[["Author", "Outlet"]]
            .copy()
            .fillna("")
        )

        assignment_map["Author"] = assignment_map["Author"].fillna("").astype(str).str.strip()
        assignment_map = assignment_map[assignment_map["Author"] != ""].copy()

        # Prefer a non-blank outlet if duplicates exist for an author
        assignment_map["has_outlet"] = assignment_map["Outlet"].str.strip().ne("")
        assignment_map = (
            assignment_map.sort_values(["Author", "has_outlet"], ascending=[True, False])
            .drop_duplicates(subset=["Author"], keep="first")
            .drop(columns=["has_outlet"])
        )

        rebuilt = rebuilt.merge(assignment_map, on="Author", how="left")
        rebuilt["Outlet"] = rebuilt["Outlet"].fillna("")
    else:
        rebuilt.insert(loc=1, column="Outlet", value="")

    if top_auths_by == "Mentions":
        rebuilt = rebuilt.sort_values(["Mentions", "Impressions"], ascending=False).reset_index(drop=True)
    else:
        rebuilt = rebuilt.sort_values(["Impressions", "Mentions"], ascending=False).reset_index(drop=True)

    desired_order = ["Author", "Outlet", "Mentions", "Impressions"]
    existing_order = [c for c in desired_order if c in rebuilt.columns]
    rebuilt = rebuilt[existing_order].copy()

    return rebuilt



def apply_author_name_fix(old_name: str, new_name: str):
    """
    Apply a corrected author name to df_traditional and rebuild auth_outlet_table.
    Keep the user on the renamed author so outlet assignment can happen next.
    """
    old_name = str(old_name).strip()
    new_name = str(new_name).strip()

    if not old_name or not new_name or old_name == new_name:
        return

    # Update source coverage data
    st.session_state.df_traditional = st.session_state.df_traditional.copy()
    st.session_state.df_traditional.loc[
        st.session_state.df_traditional["Author"] == old_name,
        "Author"
    ] = new_name

    # Rebuild grouped author table while preserving existing outlet assignments
    existing_assignments = (
        st.session_state.auth_outlet_table.copy()
        if len(st.session_state.auth_outlet_table) > 0
        else None
    )

    st.session_state.auth_outlet_table = build_auth_outlet_table(
        st.session_state.df_traditional.copy(),
        st.session_state.top_auths_by,
        existing_assignments=existing_assignments
    )

    # Keep user on the renamed author if it still needs an outlet
    auth_outlet_todo = st.session_state.auth_outlet_table.loc[
        st.session_state.auth_outlet_table["Outlet"] == ""
    ].reset_index(drop=True)

    matching_rows = auth_outlet_todo.index[auth_outlet_todo["Author"] == new_name].tolist()

    if matching_rows:
        st.session_state.auth_outlet_skipped = matching_rows[0]
    else:
        st.session_state.auth_outlet_skipped = max(
            0,
            min(st.session_state.auth_outlet_skipped, len(auth_outlet_todo) - 1)
        )

    # Keep the text input synced to the new current name
    st.session_state.author_fix_input = new_name
    st.session_state.last_author_for_fix = new_name

# def apply_author_name_fix(old_name: str, new_name: str):
#     """
#     Apply a corrected author name to df_traditional and rebuild auth_outlet_table.
#     """
#     old_name = str(old_name).strip()
#     new_name = str(new_name).strip()
#
#     if not old_name or not new_name or old_name == new_name:
#         return
#
#     # Update source coverage data
#     st.session_state.df_traditional = st.session_state.df_traditional.copy()
#     st.session_state.df_traditional.loc[
#         st.session_state.df_traditional["Author"] == old_name,
#         "Author"
#     ] = new_name
#
#     # Rebuild grouped author table while preserving existing outlet assignments
#     existing_assignments = st.session_state.auth_outlet_table.copy() if len(st.session_state.auth_outlet_table) > 0 else None
#     st.session_state.auth_outlet_table = build_auth_outlet_table(
#         st.session_state.df_traditional.copy(),
#         st.session_state.top_auths_by,
#         existing_assignments=existing_assignments
#     )
#
#     # Keep skip counter in a safe range
#     st.session_state.auth_outlet_skipped = max(0, min(
#         st.session_state.auth_outlet_skipped,
#         len(st.session_state.auth_outlet_table) - 1
#     ))


def get_matched_authors_df(search_results, outlets_in_coverage_list, author_name_for_styling):
    """
    Build dataframe of matched authors from database response.
    Also returns db_outlets and possibles lists.
    """
    db_outlets = []
    possibles = []

    if not search_results or "results" not in search_results or not search_results["results"]:
        return pd.DataFrame(), db_outlets, possibles

    response_results = search_results["results"]
    outlet_results = []

    for result in response_results:
        first = result.get("firstName", "") or ""
        last = result.get("lastName", "") or ""
        auth_name = f"{first} {last}".strip()

        primary_employment = result.get("primaryEmployment") or {}
        job_title = primary_employment.get("jobTitle", "") or ""
        outlet = primary_employment.get("outletName", "") or ""

        country_obj = result.get("country")
        country = country_obj.get("name", "") if country_obj else ""

        outlet_results.append((auth_name, job_title, outlet, country))

    matched_authors = pd.DataFrame.from_records(
        outlet_results,
        columns=["Name", "Title", "Outlet", "Country"]
    ).copy()

    if len(matched_authors) == 0:
        return matched_authors, db_outlets, possibles

    matched_authors.loc[matched_authors["Outlet"] == "[Freelancer]", "Outlet"] = "Freelance"

    db_outlets = matched_authors["Outlet"].tolist()
    possibles = matched_authors["Outlet"].tolist()

    matching_outlets = set(outlets_in_coverage_list).intersection(set(possibles))

    if len(matching_outlets) > 0 and len(possibles) > 1:
        matched_authors_top = matched_authors[matched_authors["Outlet"].isin(matching_outlets)].copy()
        matched_authors_bottom = matched_authors[~matched_authors["Outlet"].isin(matching_outlets)].copy()
        matched_authors = pd.concat([matched_authors_top, matched_authors_bottom], ignore_index=True)

        possibles = matched_authors["Outlet"].tolist()

    matching_outlet = [outlet for outlet in outlets_in_coverage_list if outlet in possibles]
    if len(matching_outlet) == 1:
        index = possibles.index(matching_outlet[0])
        possibles = [matching_outlet[0]] + possibles[:index] + possibles[index + 1:]

    return matched_authors, db_outlets, possibles



if st.session_state.get("pickle_load", False) is True and len(st.session_state.auth_outlet_table) > 0:
    st.session_state.auth_outlet_table = st.session_state.auth_outlet_table.copy()
    st.session_state.auth_outlet_table["Outlet"] = st.session_state.auth_outlet_table["Outlet"].replace([np.nan, None], "")


# defensive copy + dtype cleanup
st.session_state.df_traditional = st.session_state.df_traditional.copy()
st.session_state.df_traditional["Mentions"] = pd.to_numeric(
    st.session_state.df_traditional["Mentions"],
    errors="coerce"
).fillna(0).astype(int)

# CSS helpers
hide_table_row_index = """
    <style>
    tbody th {display:none}
    .blank {display:none}
    .row_heading.level0 {width:0; display:none}
    </style>
"""
st.markdown(hide_table_row_index, unsafe_allow_html=True)

st.session_state.top_auths_by = st.selectbox(
    "Top Authors by:",
    ["Mentions", "Impressions"],
    on_change=reset_skips
)


# Build or rebuild author-outlet table if needed
if len(st.session_state.auth_outlet_table) == 0:
    st.session_state.auth_outlet_table = build_auth_outlet_table(
        st.session_state.df_traditional.copy(),
        st.session_state.top_auths_by
    )
else:
    st.session_state.auth_outlet_table = build_auth_outlet_table(
        st.session_state.df_traditional.copy(),
        st.session_state.top_auths_by,
        existing_assignments=st.session_state.auth_outlet_table.copy()
    )

auth_outlet_todo = st.session_state.auth_outlet_table.loc[
    st.session_state.auth_outlet_table["Outlet"] == ""
].copy()

if st.session_state.auth_outlet_skipped < len(auth_outlet_todo):
    original_author_name = auth_outlet_todo.iloc[st.session_state.auth_outlet_skipped]["Author"]

    if st.session_state.get("last_author_for_fix") != original_author_name:
        st.session_state.author_fix_input = original_author_name
        st.session_state.last_author_for_fix = original_author_name

    # Editable author field for matching
    def apply_author_fix_callback():
        new_name = st.session_state.author_fix_input.strip()
        old_name = original_author_name

        if not new_name:
            return

        if new_name != old_name:
            apply_author_name_fix(old_name, new_name)


    with st.expander("Author name fix tools", expanded=False):

        st.text_input(
            "Correct author name",
            key="author_fix_input",
            on_change=apply_author_fix_callback,
            help="Edit the name and press Enter to apply the correction to all matching rows."
        )

        # st.text_input(
        #     "Correct author name",
        #     value=original_author_name,
        #     key="author_fix_input",
        #     on_change=apply_author_fix_callback,
        #     help="Edit the name and press Enter to apply the correction to all matching rows."
        # )

        st.caption(
            "This updates every instance of this author in the cleaned dataset and refreshes the author-outlet workflow."
        )


    # Current author heading
    # header_col, skip_col, reset_col = st.columns([2, 1, 1])
    header_col, skip_col, reset_col, undo_col = st.columns([2, 1, 1, 1])

    with header_col:
        st.markdown(
            f"""
            <h2 style="color: goldenrod; padding-top:0!important; margin-top:0;">
                {original_author_name}
            </h2>
            """,
            unsafe_allow_html=True
        )

    with skip_col:
        st.write(" ")
        next_auth = st.button("Skip to Next Author")
        if next_auth:
            st.session_state.auth_outlet_skipped += 1
            st.rerun()

    with reset_col:
        st.write(" ")
        reset_counter = st.button("Reset Skips")
        if reset_counter:
            st.session_state.auth_outlet_skipped = 0
            st.rerun()

    with undo_col:
        st.write(" ")
        undo_available = st.session_state.get("last_outlet_assignment") is not None
        undo_clicked = st.button(
            "Undo Last Outlet",
            disabled=not undo_available,
            help="Removes the most recently assigned outlet and returns that author to the queue."
        )
        if undo_clicked:
            undo_last_outlet_assignment()
            st.rerun()

    match_author_name = original_author_name

    # Fetch database results using the edited / cleaned name
    search_results = fetch_outlet(unidecode(match_author_name))

    # Styling helper for matched names table
    def name_match(series):
        non_match = "color: #985331;"
        match = "color: goldenrod"
        return [non_match if cell_value != match_author_name else match for cell_value in series]

    # Coverage-side outlets still based on original current dataset author until fix is applied
    outlets_in_coverage = (
        st.session_state.df_traditional.loc[
            st.session_state.df_traditional["Author"] == original_author_name,
            "Outlet"
        ]
        .value_counts()
        .rename_axis("Outlet")
        .reset_index(name="Hits")
        .copy()
    )

    outlets_in_coverage_list = pd.Index(outlets_in_coverage["Outlet"].tolist())
    outlets_in_coverage_list = outlets_in_coverage_list.insert(0, "Freelance")

    matched_authors, db_outlets, possibles = get_matched_authors_df(
        search_results=search_results,
        outlets_in_coverage_list=outlets_in_coverage_list,
        author_name_for_styling=match_author_name
    )

    form_block = st.container()
    info_block = st.container()


    with info_block:
        col1, col2, col3 = st.columns([8, 1, 16])

        with col1:
            st.subheader("Outlets in CSV")

            outlets_in_coverage_styled = outlets_in_coverage.style.apply(
                lambda x: [
                    "background-color: goldenrod; color: black" if v in db_outlets else ""
                    for v in x
                ],
                axis=1,
                subset="Outlet"
            )

            if len(outlets_in_coverage) > 7:
                st.dataframe(outlets_in_coverage_styled)
            else:
                st.table(outlets_in_coverage_styled)

        with col2:
            st.write(" ")

        with col3:
            st.subheader("Media Database Results")

            if len(matched_authors) == 0:
                st.warning("NO MATCH FOUND")
            else:
                if len(matched_authors) > 7:
                    st.dataframe(
                        matched_authors.style
                        .apply(
                            lambda x: [
                                "background: goldenrod; color: black"
                                if v in outlets_in_coverage["Outlet"].tolist() else ""
                                for v in x
                            ],
                            axis=1
                        )
                        .apply(name_match, axis=0, subset="Name")
                    )
                else:
                    st.table(
                        matched_authors.style
                        .apply(
                            lambda x: [
                                "background: goldenrod; color: black"
                                if v in outlets_in_coverage["Outlet"].tolist() else ""
                                for v in x
                            ],
                            axis=1
                        )
                        .apply(name_match, axis=0, subset="Name")
                    )

            encoded_author_name = urllib.parse.quote(match_author_name)
            muckrack_url = f"https://www.google.com/search?q=site%3Amuckrack.com+{encoded_author_name}"
            linkedin_url = f'https://www.google.com/search?q=site%3Alinkedin.com+%22{encoded_author_name}%22+journalist'

            st.markdown(
                f'&nbsp;&nbsp;» <a href="{muckrack_url}" target="_blank" style="text-decoration:underline; color:lightblue;">Search Muckrack for {match_author_name}</a>',
                unsafe_allow_html=True
            )
            st.markdown(
                f'&nbsp;&nbsp;» <a href="{linkedin_url}" target="_blank" style="text-decoration:underline; color:lightblue;">Search LinkedIn for {match_author_name}</a>',
                unsafe_allow_html=True
            )

    with form_block:
        with st.form("auth updater", clear_on_submit=True):
            col1, col2, col3 = st.columns([8, 1, 8])

            with col1:
                if len(matched_authors) > 0:
                    box_outlet = st.selectbox(
                        "Pick outlet from DATABASE MATCHES",
                        possibles,
                        help="Pick from one of the outlets associated with this author name."
                    )
                else:
                    box_outlet = st.selectbox(
                        'Pick outlet from COVERAGE or "Freelance"',
                        outlets_in_coverage_list
                    )

            with col2:
                st.write(" ")
                st.subheader("OR")

            with col3:
                string_outlet = st.text_input(
                    "Write in an outlet name",
                    help="Override the selection by writing a custom outlet name."
                )

            submitted = st.form_submit_button("Assign Outlet", type="primary")

    if submitted:
        new_outlet = string_outlet.strip() if len(string_outlet.strip()) > 0 else box_outlet

        previous_outlet_series = st.session_state.auth_outlet_table.loc[
            st.session_state.auth_outlet_table["Author"] == original_author_name,
            "Outlet"
        ]

        previous_outlet = previous_outlet_series.iloc[0] if len(previous_outlet_series) > 0 else ""

        st.session_state.last_outlet_assignment = {
            "author_name": original_author_name,
            "previous_outlet": previous_outlet,
            "previous_skip": st.session_state.auth_outlet_skipped,
        }

        st.session_state.auth_outlet_table = st.session_state.auth_outlet_table.copy()
        st.session_state.auth_outlet_table.loc[
            st.session_state.auth_outlet_table["Author"] == original_author_name,
            "Outlet"
        ] = new_outlet

        st.rerun()

    # if submitted:
    #     new_outlet = string_outlet.strip() if len(string_outlet.strip()) > 0 else box_outlet
    #
    #     st.session_state.auth_outlet_table = st.session_state.auth_outlet_table.copy()
    #     st.session_state.auth_outlet_table.loc[
    #         st.session_state.auth_outlet_table["Author"] == original_author_name,
    #         "Outlet"
    #     ] = new_outlet
    #
    #     st.rerun()

    st.divider()

    bottom_col1, bottom_col2, bottom_col3 = st.columns([8, 1, 4])

    with bottom_col1:
        st.subheader("Top Authors")

        table_df = st.session_state.auth_outlet_table[["Author", "Outlet", "Mentions", "Impressions"]].copy()
        table_df = table_df.fillna("")

        if st.session_state.top_auths_by == "Mentions":
            table_df = table_df.sort_values(["Mentions", "Impressions"], ascending=False).head(15)
        else:
            table_df = table_df.sort_values(["Impressions", "Mentions"], ascending=False).head(15)

        st.table(table_df.style.format(format_dict, na_rep=" "))

    with bottom_col2:
        st.write(" ")

    with bottom_col3:
        st.subheader("Outlets assigned")
        assigned = len(
            st.session_state.auth_outlet_table.loc[
                st.session_state.auth_outlet_table["Outlet"] != ""
            ]
        )
        st.metric(label="Assigned", value=assigned)

else:
    st.info("You've reached the end of the list!")
    st.write(f"Authors skipped: {st.session_state.auth_outlet_skipped}")

    if st.session_state.auth_outlet_skipped > 0:
        reset_counter = st.button("Reset Counter")
        if reset_counter:
            st.session_state.auth_outlet_skipped = 0
            st.rerun()
    else:
        st.write("✓ Nothing left to update here.")