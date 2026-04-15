import streamlit as st
import pandas as pd
import mig_functions as mig
import io
import warnings
import json
import zipfile
import re

warnings.filterwarnings('ignore')

st.set_page_config(
    layout="wide",
    page_title="MIG Data Processing App",
    page_icon="https://www.agilitypr.com/wp-content/uploads/2025/01/favicon.png"
)

mig.standard_sidebar()

st.title('Downloads')
mig.require_standard_pipeline()



# ---------- Helpers for main workbook ----------

def rename_ave(df: pd.DataFrame) -> pd.DataFrame:
    """Restore internal AVE column to original uploaded AVE column name for export."""
    export_ave_name = st.session_state.get("original_ave_col") or "AVE"

    if "AVE" in df.columns:
        return df.rename(columns={"AVE": export_ave_name})
    return df


def explode_tags(df: pd.DataFrame) -> pd.DataFrame:
    """Explode comma-separated Tags to one-hot columns (Categories)."""
    if "Tags" not in df.columns:
        return df
    df = df.copy()
    tags = df["Tags"].fillna("").astype(str).str.strip()
    tags = tags.str.replace(r"\s*,\s*", ",", regex=True)
    dummies = tags.str.get_dummies(sep=",")
    cleaned_columns = pd.Index([str(col).strip() for col in dummies.columns])
    dummies.columns = cleaned_columns
    dummies = dummies.loc[:, cleaned_columns != ""]
    dummies = dummies.astype("category")
    return df.join(dummies, how="left", rsuffix=" (tag)")


def build_authors_export_table(df: pd.DataFrame, existing_assignments: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Rebuild author summary from current df_traditional and preserve any assigned outlets.
    Excludes blank author names.
    """
    working = df[["Author", "Mentions", "Impressions"]].copy()

    # Normalize and exclude blank authors
    working["Author"] = working["Author"].fillna("").astype(str).str.strip()
    working = working[working["Author"] != ""].copy()

    rebuilt = (
        working.groupby("Author", as_index=False)[["Mentions", "Impressions"]]
        .sum()
    )

    if existing_assignments is not None and len(existing_assignments) > 0 and "Outlet" in existing_assignments.columns:
        assignment_map = (
            existing_assignments[["Author", "Outlet"]]
            .copy()
            .fillna("")
        )

        assignment_map["Author"] = assignment_map["Author"].fillna("").astype(str).str.strip()
        assignment_map = assignment_map[assignment_map["Author"] != ""].copy()
        assignment_map = assignment_map.drop_duplicates(subset=["Author"], keep="last")

        rebuilt = rebuilt.merge(assignment_map, on="Author", how="left")
        rebuilt["Outlet"] = rebuilt["Outlet"].fillna("")
    else:
        rebuilt.insert(loc=1, column="Outlet", value="")

    rebuilt = rebuilt[["Author", "Outlet", "Mentions", "Impressions"]].copy()
    return rebuilt


def get_currency_symbol() -> str:
    """Infer currency symbol from original uploaded AVE column name. Fallback to $."""
    original_ave_col = str(st.session_state.get("original_ave_col") or "AVE")

    if "(EUR)" in original_ave_col:
        return "€"
    if "(GBP)" in original_ave_col:
        return "£"
    if "(JPY)" in original_ave_col:
        return "¥"

    # CAD, USD, AUD, etc. can all safely fall back to $
    return "$"


def apply_sheet_column_formats(
    worksheet,
    df: pd.DataFrame,
    number_format,
    currency_format,
    sheet_type: str = "generic"
):
    """
    Apply column widths and formats by column name, not by Excel position.
    """

    if df.empty:
        return

    column_rules = {
        # common fields
        "Date": {"width": 12},
        "Published Date": {"width": 12},
        "Author": {"width": 24},
        "Outlet": {"width": 28},
        "Headline": {"width": 40},
        "Title": {"width": 40},
        "Type": {"width": 12},
        "Media Type": {"width": 12},
        "URL": {"width": 30},
        "Example URL": {"width": 30},
        "Snippet": {"width": 55},
        "Coverage Snippet": {"width": 55},
        "Summary": {"width": 45},
        "Content": {"width": 70},
        "Country": {"width": 14},
        "Prov/State": {"width": 14},
        "Language": {"width": 12},
        "Sentiment": {"width": 12},
        "Mentions": {"width": 12, "format": number_format},
        "Impressions": {"width": 14, "format": number_format},
        "AVE": {"width": 14, "format": currency_format},
        str(st.session_state.get("original_ave_col") or "AVE"): {"width": 14, "format": currency_format},

        # top stories
        "Example Outlet": {"width": 20},
        "Chart Callout": {"width": 40},
        "Top Story Summary": {"width": 55},
        "Entity Sentiment": {"width": 45},
    }

    # default row height
    worksheet.set_default_row(22)

    for col_name in df.columns:
        col_idx = df.columns.get_loc(col_name)
        rule = column_rules.get(col_name, None)

        if rule:
            width = rule.get("width", None)
            fmt = rule.get("format", None)
            worksheet.set_column(col_idx, col_idx, width, fmt)

    worksheet.freeze_panes(1, 0)


# ---------- Helper: build NotebookLM ZIP ----------

def build_notebooklm_zip(
    df_traditional: pd.DataFrame,
    df_social: pd.DataFrame,
    client_name: str,
    max_files: int = 50,
    max_rows_per_file: int = 450,
    max_words_per_file: int = 200_000,
    max_bytes_per_file: int = 50 * 1024 * 1024,
    text_truncate_len: int = 10_000
):
    """
    Build an in-memory ZIP containing JSON .txt files for NotebookLM.
    """

    frames = []
    if df_traditional is not None and len(df_traditional) > 0:
        frames.append(df_traditional)
    if df_social is not None and len(df_social) > 0:
        frames.append(df_social)

    if not frames:
        raise ValueError("No coverage rows available for NotebookLM bundle.")

    df = pd.concat(frames, ignore_index=True)

    base_cols = [
        'Published Date', 'Date', 'Author', 'Outlet', 'Headline',
        'Coverage Snippet', 'Snippet', 'Summary', 'Title', 'Content',
        'Country', 'Prov/State', 'Type', 'Media Type',
        'Impressions', 'URL', 'Sentiment', 'Tags'
    ]
    prominence_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("Prominence")]
    cols_to_keep = [c for c in base_cols if c in df.columns] + prominence_cols

    if not cols_to_keep:
        raise ValueError("No expected columns found to include in NotebookLM bundle.")

    df = df[cols_to_keep].copy()

    for date_col in ['Published Date', 'Date']:
        if date_col in df.columns:
            try:
                tmp = pd.to_datetime(df[date_col], errors='coerce')
                df[date_col] = tmp.dt.strftime('%Y-%m-%d')
            except Exception:
                pass

    text_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    for col in text_cols:
        s = df[col].astype("string")
        s = s.str.slice(0, text_truncate_len)
        df[col] = s

    df = df.astype(object)
    df = df.where(df.notna(), None)

    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    n_total_rows = len(df)

    client_clean = re.sub(r'[^A-Za-z0-9]+', '', str(client_name))
    if not client_clean:
        client_clean = "Client"
    client_short = client_clean[:15]

    output_zip = io.BytesIO()
    total_rows_included = 0
    total_words_included = 0
    files_created = 0

    def row_word_count(row_dict: dict) -> int:
        parts = []
        for col in text_cols:
            if col in row_dict:
                val = row_dict.get(col)
                if val is not None:
                    parts.append(str(val))
        if not parts:
            return 0
        return len(" ".join(parts).split())

    def make_json_safe(val):
        try:
            if pd.isna(val):
                return None
        except TypeError:
            pass
        return val

    with zipfile.ZipFile(output_zip, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        chunk_rows = []
        chunk_words = 0
        chunk_bytes = 0

        def flush_chunk(local_rows, index: int):
            nonlocal files_created
            if not local_rows:
                return
            json_str = json.dumps(local_rows, ensure_ascii=False)
            filename = f"json_{index}-{client_short}.txt"
            zf.writestr(filename, json_str)
            files_created += 1

        file_index = 1

        for _, row in df.iterrows():
            if file_index > max_files:
                break

            row_dict = {}
            for col in cols_to_keep:
                val = row[col]
                row_dict[col] = make_json_safe(val)

            w = row_word_count(row_dict)
            row_json = json.dumps(row_dict, ensure_ascii=False)
            b = len(row_json.encode('utf-8'))

            if chunk_rows and (
                len(chunk_rows) >= max_rows_per_file or
                chunk_words + w > max_words_per_file or
                chunk_bytes + b > max_bytes_per_file
            ):
                flush_chunk(chunk_rows, file_index)
                file_index += 1
                if file_index > max_files:
                    break
                chunk_rows = []
                chunk_words = 0
                chunk_bytes = 0

            if file_index <= max_files:
                chunk_rows.append(row_dict)
                chunk_words += w
                chunk_bytes += b
                total_rows_included += 1
                total_words_included += w

        if chunk_rows and file_index <= max_files:
            flush_chunk(chunk_rows, file_index)

    output_zip.seek(0)

    info = {
        "client_short": client_short,
        "total_rows": int(n_total_rows),
        "rows_included": int(total_rows_included),
        "files_created": int(files_created),
        "max_files": int(max_files),
        "max_rows_per_file": int(max_rows_per_file),
        "max_words_per_file": int(max_words_per_file),
        "max_bytes_per_file": int(max_bytes_per_file),
        "total_words_included": int(total_words_included),
    }
    return output_zip, info


# ---------- Main page logic ----------

# if not st.session_state.upload_step:
#     st.error('Please upload a CSV before trying this step.')
# elif not st.session_state.standard_step:
#     st.error('Please run the Standard Cleaning before trying this step.')
# else:
st.divider()

# local copies so we don't mutate session_state data
traditional = st.session_state.df_traditional.copy()
social = st.session_state.df_social.copy()
top_stories = st.session_state.added_df.copy()
dupes = st.session_state.df_dupes.copy()
raw = st.session_state.df_untouched.copy()

# Tag exploder for the Excel outputs
traditional = explode_tags(traditional)
social = explode_tags(social)

st.subheader("Clean data workbook")

had_clean_workbook = "clean_excel_bytes" in st.session_state

build_xlsx = st.button("Build cleaned data workbook", key="build_clean_workbook")
if build_xlsx:
    try:
        with st.spinner('Building workbook now...'):
            output = io.BytesIO()

            with pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='yyyy-mm-dd') as writer:
                workbook = writer.book
                cleaned_exports = []

                number_format = workbook.add_format({'num_format': '#,##0'})
                currency_symbol = get_currency_symbol()
                currency_format = workbook.add_format({'num_format': f'{currency_symbol}#,##0'})

                # CLEAN TRAD
                if len(traditional) > 0:
                    trad_export = rename_ave(traditional.copy())
                    trad_export = trad_export.sort_values(by=['Impressions'], ascending=False)
                    trad_export.to_excel(
                        writer,
                        sheet_name='CLEAN TRAD',
                        startrow=1,
                        header=False,
                        index=False
                    )
                    worksheet1 = writer.sheets['CLEAN TRAD']
                    worksheet1.set_tab_color('black')
                    cleaned_exports.append(("CLEAN TRAD", trad_export, worksheet1))

                # CLEAN SOCIAL
                if len(social) > 0:
                    social_export = rename_ave(social.copy())
                    social_export = social_export.sort_values(by=['Impressions'], ascending=False)
                    social_export.to_excel(
                        writer,
                        sheet_name='CLEAN SOCIAL',
                        startrow=1,
                        header=False,
                        index=False
                    )
                    worksheet2 = writer.sheets['CLEAN SOCIAL']
                    worksheet2.set_tab_color('black')
                    cleaned_exports.append(("CLEAN SOCIAL", social_export, worksheet2))

                # Authors
                authors = build_authors_export_table(
                    st.session_state.df_traditional.copy(),
                    existing_assignments=st.session_state.auth_outlet_table.copy()
                    if len(st.session_state.auth_outlet_table) > 0 else None
                )

                if len(authors) > 0:
                    authors = authors.sort_values(by=["Mentions", "Impressions"], ascending=False).copy()
                    authors.to_excel(writer, sheet_name='Authors', header=True, index=False)
                    worksheet5 = writer.sheets['Authors']
                    worksheet5.set_tab_color('green')
                    cleaned_exports.append(("Authors", authors, worksheet5))

                # Top Stories
                if len(top_stories) > 0:
                    top_stories_export = top_stories.sort_values(
                        by=['Mentions', 'Impressions'],
                        ascending=False
                    ).copy()

                    desired_top_story_columns = [
                        "Headline",
                        "Date",
                        "Mentions",
                        "Impressions",
                        "Example Outlet",
                        "Example URL",
                        "Chart Callout",
                        "Top Story Summary",
                        "Entity Sentiment",
                    ]

                    existing_top_story_columns = [
                        col for col in desired_top_story_columns if col in top_stories_export.columns
                    ]
                    top_stories_export = top_stories_export[existing_top_story_columns].copy()

                    top_stories_export.to_excel(writer, sheet_name='Top Stories', header=True, index=False)
                    worksheet6 = writer.sheets['Top Stories']
                    worksheet6.set_tab_color('green')
                    cleaned_exports.append(("Top Stories", top_stories_export, worksheet6))

                # Deleted dupes
                if len(dupes) > 0:
                    dupes_export = rename_ave(dupes.copy())
                    dupes_export.to_excel(
                        writer,
                        sheet_name='DLTD DUPES',
                        header=True,
                        index=False
                    )
                    worksheet3 = writer.sheets['DLTD DUPES']
                    worksheet3.set_tab_color('#c26f4f')
                    cleaned_exports.append(("DLTD DUPES", dupes_export, worksheet3))

                # RAW
                raw_export = rename_ave(raw.copy())
                raw_export.drop(["Mentions"], axis=1, inplace=True, errors='ignore')
                raw_export.to_excel(
                    writer,
                    sheet_name='RAW',
                    header=True,
                    index=False
                )
                worksheet4 = writer.sheets['RAW']
                worksheet4.set_tab_color('#c26f4f')
                cleaned_exports.append(("RAW", raw_export, worksheet4))

                # Add Excel table structures + dynamic formatting
                for _, clean_df, ws in cleaned_exports:
                    max_row, max_col = clean_df.shape
                    column_settings = [{'header': column} for column in clean_df.columns]
                    ws.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
                    apply_sheet_column_formats(
                        worksheet=ws,
                        df=clean_df,
                        number_format=number_format,
                        currency_format=currency_format
                    )

            # st.session_state.clean_excel_bytes = output.getvalue()
            st.session_state.clean_excel_bytes = output.getvalue()
            st.session_state.clean_excel_built_at = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            action_word = "rebuilt" if had_clean_workbook else "built"
            st.success(f"Cleaned workbook {action_word} at {st.session_state.clean_excel_built_at}")

            # st.success(f"Cleaned workbook rebuilt at {st.session_state.clean_excel_built_at}")

    except Exception as e:
        st.error(f"Error building Excel workbook: {e}")

if "clean_excel_bytes" in st.session_state:
    export_name = f"{st.session_state.export_name} - clean_data.xlsx"
    st.download_button(
        'Download cleaned data workbook',
        st.session_state.clean_excel_bytes,
        file_name=export_name,
        type="primary",
        key="download_clean_workbook"
    )

    if "clean_excel_built_at" in st.session_state:
        st.caption(f"Current workbook built: {st.session_state.clean_excel_built_at}")



st.divider()
client_name = st.session_state.client_name
# ---------- NotebookLM bundle section ----------

st.subheader("NotebookLM bundle")

had_notebooklm_bundle = "notebooklm_zip_bytes" in st.session_state

build_nlm = st.button("Build NotebookLM bundle (zip)",
                      key="build_notebooklm_bundle",
                      help=(
                          "Creates a zip of JSON-formatted text files for NotebookLM. "
                          "If the dataset exceeds ~50 files worth of content, "
                          "a random sample is taken to stay within upload limits."
                      ))
if build_nlm:
    try:
        with st.spinner("Building NotebookLM bundle..."):
            nlm_zip_io, nlm_info = build_notebooklm_zip(
                st.session_state.df_traditional,
                st.session_state.df_social,
                client_name=st.session_state.client_name  # you already wired this up
            )
        st.session_state.notebooklm_zip_bytes = nlm_zip_io.getvalue()
        st.session_state.notebooklm_info = nlm_info
        st.session_state.notebooklm_built_at = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        action_word = "rebuilt" if had_notebooklm_bundle else "built"
        st.success(f"NotebookLM bundle {action_word} at {st.session_state.notebooklm_built_at}")

        # st.success(f"NotebookLM bundle rebuilt at {st.session_state.notebooklm_built_at}")
        # st.session_state.notebooklm_zip_bytes = nlm_zip_io.getvalue()
        # st.session_state.notebooklm_info = nlm_info
    except ValueError as e:
        st.error(str(e))
    except Exception as e:
        st.error(f"Error building NotebookLM bundle: {e}")

if "notebooklm_zip_bytes" in st.session_state:
    info = st.session_state.get("notebooklm_info", {})
    client_short = info.get("client_short", "Client")
    zip_filename = f"NLM_prepared_data-{client_short}.zip"

    st.download_button(
        "Download NotebookLM bundle",
        data=st.session_state.notebooklm_zip_bytes,
        file_name=zip_filename,
        type="primary"
    )
    if "notebooklm_built_at" in st.session_state:
        st.caption(f"Current NotebookLM bundle built: {st.session_state.notebooklm_built_at}")

    if info:
        st.caption(
            f"Rows in cleaned dataset: {info.get('total_rows', 0):,} · "
            f"Rows included in bundle: {info.get('rows_included', 0):,} · "
            f"Files: {info.get('files_created', 0)} (max {info.get('max_files', 0)}), "
            f"Rows/file cap: {info.get('max_rows_per_file', 0)}, "
            f"Word limit/file: {info.get('max_words_per_file', 0):,}, "
            f"Size limit/file: {info.get('max_bytes_per_file', 0) // (1024 * 1024)} MB."
        )

st.divider()
client_name = st.session_state.client_name
# ---------- NotebookLM prompt examples ----------

with st.expander("NotebookLM prompt examples for this client"):
    st.markdown(
        f"""
### Executive Summary
Generate a concise 2-paragraph executive summary of the media coverage of **{client_name}**.  
Present the information as though it is going to be included as a lead into a media briefing for an executive.  
Focus on high-level concepts rather than specific facts and figures.

---

### High-level coverage summary

You are analyzing media coverage for **{client_name}**.  
Summarize the overall coverage tone, key themes, and notable storylines across this dataset.  
Highlight the most influential outlets and authors, and note any spikes in volume, sentiment shifts, or recurring issues.

---

### Coverage Themes
What are the 5–7 key themes in the coverage that would be pertinent to **{client_name}**, and the communications and public relations professionals who work there?  
For each theme, include **5 example headlines** of where that topic was found. For each headline, also include the **date and outlet**.

---

### Competitive Comparison
Are any of **{client_name}**’s competitors mentioned in any of the stories?  
If so, how does the media tend to characterize them? How are they compared to **{client_name}**?

---

### SWOT Analysis (Media Coverage Perspective)

Task  
Analyze the news articles and broadcast transcripts related to **{client_name}** contained in this notebook.  
Generate a media-coverage-driven SWOT analysis that reflects how **{client_name}** is positioned through earned media, based solely on observable patterns, themes, and narratives in the coverage.

Audience & Perspective  
Write for communications and PR professionals responsible for understanding how organizational reputation is shaped in the media.  
Act strictly as a **media analyst**, not as a strategist or advisor.

Do not:
- Recommend actions, tactics, messaging, or strategy  
- Suggest what the organization “should” do  
- Speculate beyond what is supported by coverage evidence  

Analytical Framing Rules  
- Base all points on **coverage patterns**, not assumptions about operations or intent  
- Use neutral analytical phrasing such as:
- “coverage reflects…”
- “media frequently positions…”
- “reporting emphasizes…”  
- Avoid consulting or marketing jargon (e.g., “own the narrative,” “optimize messaging,” “capitalize on”)  
- Where relevant, distinguish between:
- **Story volume**
- **Story type**
- **Audience reach / outlet credibility**
as drivers of reputational impact  

SWOT Structure  

Provide **4–5 concise bullets per quadrant**, written in clear, plain language and consistent in specificity.

**Strengths**  
Positive reputational signals conveyed through coverage (e.g., authority positioning, association with innovation or leadership, visibility in high-credibility outlets, trusted expert commentary).

**Weaknesses**  
Reputational vulnerabilities or negative associations reflected in coverage (e.g., incident-driven reporting, legal or safety narratives, uneven sub-brand perception, contextual linkage to negative events).

**Opportunities**  
Gaps, under-developed narratives, or emerging themes suggesting potential for broader or more balanced reputational positioning — without implying recommended action.

**Threats**  
External or category-level narratives present in coverage that may pose reputational risk (e.g., regulatory scrutiny, cybersecurity issues, political framing, sector instability).

Observations Section  

After the SWOT bullets, write a brief **“SWOT Analysis Observations”** section.  
Include **one short paragraph per quadrant** synthesizing what the coverage patterns suggest at a higher level.

These observations should:
- Explain why the patterns matter from a media-analysis standpoint  
- Not repeat bullets verbatim  
- Not introduce new unsupported claims  

Tone & Constraints  
- Analytical, neutral, evidence-based  
- No prescriptive or advisory language  
- No operational judgments  
- No generalized industry claims unless clearly reflected in the sources  


---

### Issues and Risk Monitoring
Review the coverage of {client_name} and describe any emerging or ongoing issues or risks that could affect the organization’s reputation.
Present your findings as concise, clearly structured text (not tables).
For each issue, include:
- A brief summary of what the issue is about
- An estimate of how common it is in the dataset (frequent / occasional / rare)
- The general sentiment or framing of the coverage (positive / neutral / negative)
- A few representative headlines or short quotes that illustrate the tone or context
- A sentence or two on whether the issue appears to be growing, stable, or fading in prominence

---

### Implications and Recommendations for Comms
Based on the coverage patterns for {client_name}, outline observations and cautious considerations that might inform communications planning — without prescribing strategy.
Write your response as concise, clearly structured text (not tables).
For example, you might note:
- Which narratives appear most influential or persistent
- Any patterns that might warrant closer monitoring or further validation
- Possible areas where proactive engagement or clarification could help manage perceptions
- Observations that may be useful for future reporting or research focus

---

### Misinformation & False Claims
Search the coverage of {client_name} for any mentions of false, misleading, or unverified information about the brand itself.
Exclude stories where {client_name} is the one accused of misleading others.
For each example, describe the nature of the misinformation, its origin (if known), and how the media handled it — corrected, ignored, or amplified it.
Present your findings as structured text sections, not as a table.

---

### Negative Message Discovery
You are analyzing media coverage of {client_name}.
Ignore any existing sentiment labels or numerical scores in the data.
Your goal is to identify and describe any negative messages or narratives about {client_name} that appear across the coverage, from the obvious to the more burried.

For each distinct negative message you find:
- Start a new short section with a clear heading naming the message (e.g., “Concerns about Product Safety”, “Leadership Controversies”).
- Summarize what the message is and how it is expressed in the media (2–3 sentences).
- Explain what aspect of {client_name} it relates to (e.g., reputation, products, financials, operations, ethics, customer experience, etc.).
- Include one or two short quotes or paraphrased examples, with outlet names and dates mentioned inline where available.
- Indicate whether the message appears isolated (few stories) or recurring (multiple outlets, ongoing).

At the end, include a short paragraph summarizing what these negative messages collectively suggest about how {client_name} is being portrayed, without speculating about motives or offering recommendations.
Present your findings as structured text sections, not as a table.

"""

)
