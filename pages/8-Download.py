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
st.divider()


# ---------- Helpers for main workbook ----------

def rename_ave(df: pd.DataFrame) -> pd.DataFrame:
    """Safely rename AVE -> configured AVE column name, if AVE exists."""
    ave_name = st.session_state.ave_col[0]
    if 'AVE' in df.columns:
        return df.rename(columns={'AVE': ave_name})
    return df


def explode_tags(df: pd.DataFrame) -> pd.DataFrame:
    """Explode comma-separated Tags to one-hot columns (Categories)."""
    if "Tags" not in df.columns:
        return df
    df = df.copy()
    df["Tags"] = df["Tags"].astype(str)
    dummies = df["Tags"].str.get_dummies(sep=",").astype("category")
    return df.join(dummies, how="left", rsuffix=" (tag)")


# ---------- Helper: build NotebookLM ZIP ----------

def build_notebooklm_zip(
    df_traditional: pd.DataFrame,
    df_social: pd.DataFrame,
    client_name: str,
    max_files: int = 50,
    max_rows_per_file: int = 500,          # conservative row cap per file
    max_words_per_file: int = 120_000,     # conservative word cap
    max_bytes_per_file: int = 25 * 1024 * 1024,  # ~25 MB
    text_truncate_len: int = 10_000
):
    """
    Build an in-memory ZIP containing JSON .txt files for NotebookLM.

    - Uses df_traditional and df_social combined (no df_untouched).
    - Keeps a fixed set of columns (base list + Tags + Prominence*).
    - Truncates all text columns to text_truncate_len chars.
    - Streams rows into chunks respecting row/word/byte limits and max_files.
    - If data can't fully fit, output is a random sample (due to shuffling).
    """

    frames = []
    if df_traditional is not None and len(df_traditional) > 0:
        frames.append(df_traditional)
    if df_social is not None and len(df_social) > 0:
        frames.append(df_social)

    if not frames:
        raise ValueError("No coverage rows available for NotebookLM bundle.")

    df = pd.concat(frames, ignore_index=True)

    # Columns to keep
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

    # Normalize dates to strings where possible
    for date_col in ['Published Date', 'Date']:
        if date_col in df.columns:
            try:
                tmp = pd.to_datetime(df[date_col], errors='coerce')
                df[date_col] = tmp.dt.strftime('%Y-%m-%d')
            except Exception:
                # If conversion fails, leave as is
                pass

    # Truncate all text-like columns
    text_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    for col in text_cols:
        s = df[col].astype("string")
        s = s.str.slice(0, text_truncate_len)
        df[col] = s

    # Force to plain Python objects and replace NA/NaN with None
    df = df.astype(object)
    df = df.where(df.notna(), None)

    # Shuffle once so truncation at max_files is effectively a random sample
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    n_total_rows = len(df)

    # Short, safe client name for filenames
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

    # Extra guard: normalize values to JSON-safe types
    def make_json_safe(val):
        try:
            if pd.isna(val):
                return None
        except TypeError:
            # Some types (e.g. dict) can throw in isna; leave them as-is
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

            # Build row dict and sanitize values
            row_dict = {}
            for col in cols_to_keep:
                val = row[col]
                row_dict[col] = make_json_safe(val)

            w = row_word_count(row_dict)
            row_json = json.dumps(row_dict, ensure_ascii=False)
            b = len(row_json.encode('utf-8'))

            # If adding this row would exceed any limit and we already have some rows, flush first
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

        # Flush final chunk if any
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

if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')
elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')
else:
    # local copies so we don't mutate session_state data
    traditional = st.session_state.df_traditional.copy()
    social = st.session_state.df_social.copy()
    auth_outlet_table = st.session_state.auth_outlet_table.copy()
    top_stories = st.session_state.added_df.copy()
    dupes = st.session_state.df_dupes.copy()
    raw = st.session_state.df_untouched.copy()

    # Tag exploder for the Excel outputs (on the copies)
    traditional = explode_tags(traditional)
    social = explode_tags(social)

    # ---------- Cleaned workbook section (no form) ----------

    st.subheader("Clean data workbook")

    build_xlsx = st.button("Build cleaned data workbook")
    if build_xlsx:
        try:
            with st.spinner('Building workbook now...'):
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='yyyy-mm-dd') as writer:

                    workbook = writer.book
                    cleaned_dfs = []
                    cleaned_sheets = []

                    # Cell formats
                    number_format = workbook.add_format({'num_format': '#,##0'})
                    currency_format = workbook.add_format({'num_format': '$#,##0'})

                    # CLEAN TRAD
                    if len(traditional) > 0:
                        traditional = rename_ave(traditional)
                        traditional = traditional.sort_values(by=['Impressions'], ascending=False)
                        traditional.to_excel(
                            writer,
                            sheet_name='CLEAN TRAD',
                            startrow=1,
                            header=False,
                            index=False
                        )
                        worksheet1 = writer.sheets['CLEAN TRAD']
                        worksheet1.set_tab_color('black')
                        cleaned_dfs.append((traditional, worksheet1))
                        cleaned_sheets.append(worksheet1)

                    # CLEAN SOCIAL
                    if len(social) > 0:
                        social = rename_ave(social)
                        social = social.sort_values(by=['Impressions'], ascending=False)
                        social.to_excel(
                            writer,
                            sheet_name='CLEAN SOCIAL',
                            startrow=1,
                            header=False,
                            index=False
                        )
                        worksheet2 = writer.sheets['CLEAN SOCIAL']
                        worksheet2.set_tab_color('black')
                        cleaned_dfs.append((social, worksheet2))
                        cleaned_sheets.append(worksheet2)

                    # Authors
                    if len(auth_outlet_table) > 0:
                        authors = auth_outlet_table.sort_values(
                            by=['Mentions', 'Impressions'],
                            ascending=False
                        )
                        authors.to_excel(writer, sheet_name='Authors', header=True, index=False)
                        worksheet5 = writer.sheets['Authors']
                        worksheet5.set_tab_color('green')
                        worksheet5.set_default_row(22)
                        worksheet5.set_column('A:A', 30, None)       # author
                        worksheet5.set_column('C:C', 12, None)       # mentions
                        worksheet5.set_column('D:D', 15, number_format)  # impressions
                        worksheet5.set_column('B:B', 35, None)       # outlet
                        worksheet5.freeze_panes(1, 0)
                        cleaned_dfs.append((authors, worksheet5))

                    # Top Stories
                    if len(top_stories) > 0:
                        top_stories = top_stories.sort_values(
                            by=['Mentions', 'Impressions'],
                            ascending=False
                        )
                        top_stories.to_excel(writer, sheet_name='Top Stories', header=True, index=False)
                        worksheet6 = writer.sheets['Top Stories']
                        worksheet6.set_tab_color('green')
                        worksheet6.set_column('A:A', 35, None)          # headline
                        worksheet6.set_column('B:B', 12, None)          # date
                        worksheet6.set_column('C:C', 12, number_format) # mentions
                        worksheet6.set_column('D:D', 12, number_format) # impressions
                        worksheet6.set_column('E:E', 20, None)          # outlet
                        worksheet6.set_column('F:F', 15, None)          # url
                        worksheet6.set_column('G:G', 15, None)          # type
                        worksheet6.set_column('H:H', 15, None)          # snippet
                        worksheet6.set_column('I:I', 40, None)          # summary
                        worksheet6.set_column('J:J', 40, None)          # sentiment
                        worksheet6.freeze_panes(1, 0)
                        cleaned_dfs.append((top_stories, worksheet6))

                    # Deleted dupes
                    if len(dupes) > 0:
                        dupes = rename_ave(dupes)
                        dupes.to_excel(
                            writer,
                            sheet_name='DLTD DUPES',
                            header=True,
                            index=False
                        )
                        worksheet3 = writer.sheets['DLTD DUPES']
                        worksheet3.set_tab_color('#c26f4f')
                        cleaned_dfs.append((dupes, worksheet3))
                        cleaned_sheets.append(worksheet3)

                    # RAW (untouched)
                    raw = rename_ave(raw)
                    raw.drop(
                        ["Mentions"],
                        axis=1,
                        inplace=True,
                        errors='ignore'
                    )
                    raw.to_excel(
                        writer,
                        sheet_name='RAW',
                        header=True,
                        index=False
                    )
                    worksheet4 = writer.sheets['RAW']
                    worksheet4.set_tab_color('#c26f4f')
                    cleaned_dfs.append((raw, worksheet4))

                    # Add table structures
                    for clean_df, ws in cleaned_dfs:
                        (max_row, max_col) = clean_df.shape
                        column_settings = [{'header': column} for column in clean_df.columns]
                        ws.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

                    # Styling for main cleaned sheets
                    for sheet in cleaned_sheets:
                        sheet.set_default_row(22)
                        sheet.set_column('A:A', 12, None)           # datetime
                        sheet.set_column('B:B', 12, None)           # author
                        sheet.set_column('C:C', 22, None)           # outlet
                        sheet.set_column('D:D', 40, None)           # headline
                        sheet.set_column('E:E', 0, None)            # mentions
                        sheet.set_column('F:F', 12, number_format)  # impressions
                        sheet.set_column('L:L', 10, None)           # type
                        sheet.set_column('Q:Q', 12, currency_format)  # AVE
                        sheet.freeze_panes(1, 0)

                # store bytes for download button
                st.session_state.clean_excel_bytes = output.getvalue()
        except Exception as e:
            st.error(f"Error building Excel workbook: {e}")

    if "clean_excel_bytes" in st.session_state:
        export_name = f"{st.session_state.export_name} - clean_data.xlsx"
        st.download_button(
            'Download cleaned data workbook',
            st.session_state.clean_excel_bytes,
            file_name=export_name,
            type="primary"
        )

    st.divider()
    # ---------- NotebookLM bundle section ----------

    st.subheader("NotebookLM bundle")

    build_nlm = st.button("Build NotebookLM bundle (zip)",
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

        if info:
            st.caption(
                f"Rows in cleaned dataset: {info.get('total_rows', 0):,} · "
                f"Rows included in bundle: {info.get('rows_included', 0):,} · "
                f"Files: {info.get('files_created', 0)} (max {info.get('max_files', 0)}), "
                f"Rows/file cap: {info.get('max_rows_per_file', 0)}, "
                f"Word limit/file: {info.get('max_words_per_file', 0):,}, "
                f"Size limit/file: {info.get('max_bytes_per_file', 0) // (1024 * 1024)} MB."
            )