import streamlit as st
import pandas as pd
import mig_functions as mig
import re
import warnings

warnings.filterwarnings('ignore')
st.set_page_config(layout="wide", page_title="MIG Data Processing App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")

mig.standard_sidebar()

st.title('Getting Started')

# Initialize Session State Variables
string_vars = {'top_auths_by': 'Mentions', 'export_name': '', 'client_name': '', 'auth_skip_counter': 0,
               'auth_outlet_skipped': 0, }
for key, value in string_vars.items():
    if key not in st.session_state:
        st.session_state[key] = value

st.session_state.df_names = ['df_traditional', 'df_social', 'df_dupes', 'auth_outlet_table',
                             'df_untouched', 'author_outlets', 'blank_set', 'added_df', 'markdown_content',
                             'filtered_df', 'df_grouped', 'selected_df', 'selected_rows', 'top_stories',
                             'auth_outlet_todo']
for _ in st.session_state.df_names:
    if _ not in st.session_state:
        st.session_state[_] = pd.DataFrame()

step_vars = ['upload_step', 'standard_step', 'translated_headline', 'translated_summary', 'translated_snippet',
             'filled', 'pickle_load']
for _ in step_vars:
    if _ not in st.session_state:
        st.session_state[_] = False

text_vars = ['ave_col']
for _ in text_vars:
    if _ not in st.session_state:
        st.session_state[_] = 'AVE'


# UPLOAD STEP
if not st.session_state.upload_step:

    client = st.text_input('Client organization name*', placeholder='eg. Air Canada', key='client',
                           help='Required to build export file name.')
    period = st.text_input('Reporting period or focus*', placeholder='eg. March 2022', key='period',
                           help='Required to build export file name.')
    uploaded_file = st.file_uploader(label='Upload your CSV or XLSX*', type=['csv', 'xlsx'],
                                     accept_multiple_files=False,
                                     help='Only use CSV files exported from the Agility Platform or XLSX files produced by this app.')

    if uploaded_file is not None:
        if uploaded_file.type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            # Read the xlsx file
            excel_file = pd.ExcelFile(uploaded_file)

            # Get the sheet names and select if necessary
            sheet_names = excel_file.sheet_names
            if len(sheet_names) > 1:
                sheet = st.selectbox('Select a sheet:', sheet_names)
            else:
                sheet = sheet_names[0]

            # Read only the selected sheet
            st.session_state.df_untouched = pd.read_excel(excel_file, sheet_name=sheet)

        elif uploaded_file.type == 'text/csv':
            # Use chunksize for large files
            chunk_list = []
            for chunk in pd.read_csv(uploaded_file, chunksize=5000):
                chunk_list.append(chunk)
            st.session_state.df_untouched = pd.concat(chunk_list, ignore_index=True)


    # if not uploaded_file == None:
    #     if uploaded_file.type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
    #         # Read the xlsx file
    #         excel_file = pd.ExcelFile(uploaded_file)
    #         # Get the sheet names
    #         sheet_names = excel_file.sheet_names
    #         # If there is more than one sheet, let the user select which one to use
    #         if len(sheet_names) > 1:
    #
    #             sheet = st.selectbox('Select a sheet:', sheet_names)
    #             st.session_state.df_untouched = pd.read_excel(excel_file, sheet_name=sheet)
    #         else:
    #             st.session_state.df_untouched = pd.read_excel(excel_file)
    #     elif uploaded_file.type == 'text/csv':
    #         st.session_state.df_untouched = pd.read_csv(uploaded_file)

    submitted = st.button("Submit", type="primary")

    if submitted and (client == "" or period == "" or uploaded_file is None):
        st.error('Missing required form inputs above.')

    elif submitted:
        with st.spinner("Converting file format."):

            st.session_state.df_traditional = st.session_state.df_untouched.dropna(thresh=3)

            # st.session_state.df_traditional = st.session_state.df_traditional.dropna(thresh=3)
            st.session_state.df_traditional["Mentions"] = 1

            st.session_state.df_traditional['Impressions'] = st.session_state.df_traditional['Impressions'].astype(
                'Int64')

            # DEALING WITH VARIABLE AVE NAMES
            # assume the original DataFrame is stored in a variable called `df`
            # find the column with the "AVE" and currency type in parentheses, or just "AVE"
            st.session_state.ave_col = [col for col in st.session_state.df_traditional.columns if
                                        re.match(r'^AVE\([A-Z]{2,3}\)$', col)] or [col for col in
                                                                                   st.session_state.df_traditional.columns
                                                                                   if col == "AVE"]

            # rename the column to "AVE" for data cleaning
            st.session_state.df_traditional = st.session_state.df_traditional.rename(
                columns={st.session_state.ave_col[0]: 'AVE'})

            st.session_state.df_traditional['AVE'] = st.session_state.df_traditional['AVE'].fillna(0)
            st.session_state.export_name = f"{client} - {period}"
            st.session_state.client_name = client

            # st.session_state.df_traditional = st.session_state.df_traditional
            st.session_state.df_traditional.drop(["Timezone",
                                                  "Word Count",
                                                  "Duration",
                                                  "Image URLs",
                                                  "Folders",
                                                  "Notes",
                                                  "County",
                                                  "Saved Date",
                                                  "Edited Date"],
                                                 axis=1, inplace=True, errors='ignore')

            columns_to_change = ["Sentiment", "Continent", "Country", "Province/State", "Prov/State", "City",
                                 "Language"]

            for column in columns_to_change:
                if column in st.session_state.df_traditional.columns:
                    st.session_state.df_traditional[column] = st.session_state.df_traditional[column].astype('category')

            if "Published Date" in st.session_state.df_traditional.columns:
                st.session_state.df_traditional['Date'] = pd.to_datetime(
                    st.session_state.df_traditional['Published Date'] + ' ' +
                    st.session_state.df_traditional['Published Time'], errors='coerce'
                )
                st.session_state.df_traditional.drop(["Published Date", "Published Time"], axis=1, inplace=True,
                                                     errors='ignore')

            # Move the Date column to the front
            if 'Date' in st.session_state.df_traditional.columns:
                first_column = st.session_state.df_traditional.pop('Date')
                st.session_state.df_traditional.insert(0, 'Date', first_column)


            # if "Published Date" in st.session_state.df_traditional:
            #     st.session_state.df_traditional['Date'] = pd.to_datetime(
            #         st.session_state.df_traditional['Published Date'] + ' ' + st.session_state.df_traditional[
            #             'Published Time'])
            #     st.session_state.df_traditional.drop(["Published Date", "Published Time"], axis=1, inplace=True,
            #                                          errors='ignore')
            #
            #     first_column = st.session_state.df_traditional.pop('Date')
            #     st.session_state.df_traditional.insert(0, 'Date', first_column)

            st.session_state.df_traditional = st.session_state.df_traditional.rename(columns={
                'Media Type': 'Type',
                'Coverage Snippet': 'Snippet',
                'Province/State': 'Prov/State',
            })

            st.session_state.upload_step = True
            st.rerun()

# POST UPLOAD STEP
if st.session_state.upload_step:
    st.success('File uploaded.')
    if st.button('Start Over?'):
        for key in st.session_state.keys():
            del st.session_state[key]
        st.rerun()

    st.session_state.df_untouched["Mentions"] = 1

    st.header('Initial Stats')

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Mentions", value="{:,}".format(len(st.session_state.df_traditional.dropna(thresh=3))))
        impressions = st.session_state.df_traditional['Impressions'].sum()
        st.metric(label="Impressions", value=mig.format_number(impressions))
        media_type_column = "Type" if "Type" in st.session_state.df_traditional.columns else "Media Type"
        st.write(st.session_state.df_traditional[media_type_column].value_counts())
    with col2:
        st.subheader("Top Authors")
        original_top_authors = (mig.top_x_by_mentions(st.session_state.df_traditional, "Author"))
        st.write(original_top_authors)
    with col3:
        st.subheader("Top Outlets")
        original_top_outlets = (mig.top_x_by_mentions(st.session_state.df_traditional, "Outlet"))
        st.write(original_top_outlets)

    df = st.session_state.df_traditional.copy()
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    summary_stats = df.groupby("Date").agg({"Date": "count", "Impressions": "sum"})
    summary_stats.rename(columns={"Date": "Mentions"}, inplace=True)
    summary_stats.reset_index(inplace=True)

    col1, col2 = st.columns(2, gap="large")
    with col1:
        st.subheader('Mention Trend')
        st.area_chart(data=summary_stats, x="Date", y="Mentions", width=0, height=250, use_container_width=True)
    with col2:
        st.subheader('Impressions Trend')
        st.area_chart(data=summary_stats, x="Date", y="Impressions", width=0, height=250, use_container_width=True)