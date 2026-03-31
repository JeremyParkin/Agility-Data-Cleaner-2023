import streamlit as st
import pandas as pd
import mig_functions as mig
import re
import warnings

warnings.filterwarnings("ignore")

st.set_page_config(
    layout="wide",
    page_title="MIG Data Processing App",
    page_icon="https://www.agilitypr.com/wp-content/uploads/2025/01/favicon.png"
)

mig.standard_sidebar()

st.title("Getting Started")


def detect_original_ave_col(df: pd.DataFrame) -> str | None:
    """
    Detect the original AVE column name from uploaded data, such as:
    AVE(USD), AVE(CAD), or AVE.
    """
    ave_candidates = [
        col for col in df.columns if re.match(r"^AVE\([A-Z]{2,3}\)$", str(col))
    ]
    if not ave_candidates and "AVE" in df.columns:
        ave_candidates = ["AVE"]
    return ave_candidates[0] if ave_candidates else None


def normalize_uploaded_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize uploaded CSV/XLSX data into a consistent internal schema
    regardless of whether the file is raw, app-produced, or partially transformed.
    """
    df = df.copy()

    # Drop near-empty rows early
    df = df.dropna(thresh=3).copy()

    # Standardize likely variant column names into one schema
    rename_map = {
        "Media Type": "Type",
        "Coverage Snippet": "Snippet",
        "Province/State": "Prov/State",
    }
    df.rename(columns=rename_map, inplace=True)

    # Add Mentions if missing
    if "Mentions" not in df.columns:
        df["Mentions"] = 1

    # Normalize Impressions safely
    if "Impressions" in df.columns:
        df["Impressions"] = pd.to_numeric(
            df["Impressions"], errors="coerce"
        ).fillna(0).astype("Int64")

    # Normalize AVE column variants to internal working name: AVE
    ave_candidates = [
        col for col in df.columns if re.match(r"^AVE\([A-Z]{2,3}\)$", str(col))
    ]
    if not ave_candidates and "AVE" in df.columns:
        ave_candidates = ["AVE"]

    if ave_candidates:
        original_ave_col = ave_candidates[0]
        df.rename(columns={original_ave_col: "AVE"}, inplace=True)
        df["AVE"] = pd.to_numeric(df["AVE"], errors="coerce").fillna(0)

    # Build unified Date column if needed
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    elif "Published Date" in df.columns and "Published Time" in df.columns:
        published_date = pd.to_datetime(df["Published Date"], errors="coerce")
        published_time = df["Published Time"].fillna("").astype(str).str.strip()

        df["Date"] = pd.to_datetime(
            published_date.dt.strftime("%Y-%m-%d").fillna("") + " " + published_time,
            errors="coerce"
        )
    elif "Published Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Published Date"], errors="coerce")

    # Drop raw date columns once Date exists
    if "Date" in df.columns:
        df.drop(["Published Date", "Published Time"], axis=1, inplace=True, errors="ignore")

    # Normalize text-like columns safely
    text_columns = [
        "Headline", "Snippet", "Outlet", "Author", "URL", "Type",
        "Sentiment", "Continent", "Country", "Prov/State", "City", "Language"
    ]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    # Drop columns not needed for app processing
    df.drop(
        [
            "Timezone",
            "Word Count",
            "Duration",
            "Image URLs",
            "Folders",
            "Notes",
            "County",
            "Saved Date",
            "Edited Date",
        ],
        axis=1,
        inplace=True,
        errors="ignore"
    )

    # Move Date to front
    if "Date" in df.columns:
        date_col = df.pop("Date")
        df.insert(0, "Date", date_col)

    return df


def read_uploaded_file(uploaded_file) -> pd.DataFrame:
    """
    Read uploaded CSV or XLSX into a dataframe.
    """
    if uploaded_file is None:
        return pd.DataFrame()

    file_name = uploaded_file.name.lower()

    if file_name.endswith(".xlsx"):
        excel_file = pd.ExcelFile(uploaded_file)
        sheet_names = excel_file.sheet_names

        if len(sheet_names) > 1:
            sheet = st.selectbox("Select a sheet:", sheet_names)
        else:
            sheet = sheet_names[0]

        return pd.read_excel(excel_file, sheet_name=sheet)

    if file_name.endswith(".csv"):
        chunk_list = []
        for chunk in pd.read_csv(uploaded_file, chunksize=5000):
            chunk_list.append(chunk)
        return pd.concat(chunk_list, ignore_index=True)

    return pd.DataFrame()


# Initialize Session State Variables
string_vars = {
    "top_auths_by": "Mentions",
    "export_name": "",
    "client_name": "",
    "auth_skip_counter": 0,
    "auth_outlet_skipped": 0,
}
for key, value in string_vars.items():
    if key not in st.session_state:
        st.session_state[key] = value

st.session_state.df_names = [
    "df_traditional",
    "df_social",
    "df_dupes",
    "auth_outlet_table",
    "df_untouched",
    "author_outlets",
    "blank_set",
    "added_df",
    "markdown_content",
    "filtered_df",
    "df_grouped",
    "selected_df",
    "selected_rows",
    "top_stories",
    "auth_outlet_todo",
    "df_traditional_pre_standard",
]
for name in st.session_state.df_names:
    if name not in st.session_state:
        st.session_state[name] = pd.DataFrame()

step_vars = [
    "upload_step",
    "standard_step",
    "translated_headline",
    "translated_summary",
    "translated_snippet",
    "filled",
    "pickle_load",
]
for name in step_vars:
    if name not in st.session_state:
        st.session_state[name] = False

if "ave_col" not in st.session_state:
    st.session_state["ave_col"] = "AVE"

if "original_ave_col" not in st.session_state:
    st.session_state["original_ave_col"] = None

# UPLOAD STEP
if not st.session_state.upload_step:
    client = st.text_input(
        "Client organization name*",
        placeholder="eg. Air Canada",
        key="client",
        help="Required to build export file name.",
    )
    period = st.text_input(
        "Reporting period or focus*",
        placeholder="eg. March 2022",
        key="period",
        help="Required to build export file name.",
    )
    uploaded_file = st.file_uploader(
        label="Upload your CSV or XLSX*",
        type=["csv", "xlsx"],
        accept_multiple_files=False,
        help="Use CSV files exported from the Agility Platform or XLSX files produced by this app.",
    )

    if uploaded_file is not None:
        st.session_state.df_untouched = read_uploaded_file(uploaded_file)

    submitted = st.button("Submit", type="primary")

    if submitted and (client == "" or period == "" or uploaded_file is None):
        st.error("Missing required form inputs above.")

    elif submitted:
        with st.spinner("Converting file format."):
            # Preserve original uploaded AVE name for later export restoration
            st.session_state.original_ave_col = detect_original_ave_col(
                st.session_state.df_untouched
            )

            st.session_state.df_traditional = normalize_uploaded_dataframe(
                st.session_state.df_untouched
            )
            st.session_state.df_traditional_pre_standard = st.session_state.df_traditional.copy()

            # Internal working name for rest of app
            if "AVE" in st.session_state.df_traditional.columns:
                st.session_state.ave_col = "AVE"

            # Convert some columns to category after normalization
            category_columns = [
                "Sentiment",
                "Continent",
                "Country",
                "Prov/State",
                "City",
                "Language",
            ]
            for column in category_columns:
                if column in st.session_state.df_traditional.columns:
                    st.session_state.df_traditional[column] = st.session_state.df_traditional[column].astype("category")

            st.session_state.export_name = f"{client} - {period}"
            st.session_state.client_name = client
            st.session_state.upload_step = True
            st.rerun()

# POST UPLOAD STEP
if st.session_state.upload_step:
    st.success("File uploaded.")

    if st.button("Start Over?"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    st.header("Initial Stats")

    # Use normalized copy for display logic too, so stats work consistently across file types
    # df_display = normalize_uploaded_dataframe(st.session_state.df_untouched.copy())

    # Reuse the already-normalized pre-standard dataframe instead of reprocessing on every rerun
    if not st.session_state.df_traditional_pre_standard.empty:
        df_display = st.session_state.df_traditional_pre_standard.copy()
    else:
        # fallback safeguard
        df_display = st.session_state.df_traditional.copy()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(label="Mentions", value="{:,}".format(len(df_display)))

        impressions = df_display["Impressions"].sum() if "Impressions" in df_display.columns else 0
        st.metric(label="Impressions", value=mig.format_number(impressions))

        if "Type" in df_display.columns:
            st.write(df_display["Type"].value_counts())
        else:
            st.info("No media type column available.")

    with col2:
        st.subheader("Top Authors")
        if "Author" in df_display.columns:
            authors_df = df_display.copy()
            authors_df["Author"] = authors_df["Author"].fillna("").astype(str).str.strip()
            authors_df = authors_df[authors_df["Author"] != ""].copy()

            if not authors_df.empty:
                original_top_authors = mig.top_x_by_mentions(authors_df, "Author")
                st.write(original_top_authors)
            else:
                st.info("No non-blank authors available.")
        else:
            st.info("No Author column available.")

    with col3:
        st.subheader("Top Outlets")
        if "Outlet" in df_display.columns:
            original_top_outlets = mig.top_x_by_mentions(df_display, "Outlet")
            st.write(original_top_outlets)
        else:
            st.info("No Outlet column available.")

    df_trend = df_display.copy()

    if "Date" in df_trend.columns:
        df_trend["Date"] = pd.to_datetime(df_trend["Date"], errors="coerce").dt.date
    else:
        df_trend["Date"] = pd.NaT

    if df_trend["Date"].notna().any():
        summary_stats = df_trend.groupby("Date", dropna=True).agg(
            Mentions=("Mentions", "sum"),
            Impressions=("Impressions", "sum") if "Impressions" in df_trend.columns else ("Date", "size"),
        ).reset_index()

        col1, col2 = st.columns(2, gap="large")

        with col1:
            st.subheader("Mention Trend")
            st.area_chart(
                data=summary_stats,
                x="Date",
                y="Mentions",
                height=250,
                use_container_width=True,
                # width="stretch",
            )

        with col2:
            st.subheader("Impressions Trend")
            if "Impressions" in summary_stats.columns:
                st.area_chart(
                    data=summary_stats,
                    x="Date",
                    y="Impressions",
                    height=250,
                    use_container_width=True,
                    # width="stretch",
                )
    else:
        st.info("No date information available to display mention and impressions trends.")