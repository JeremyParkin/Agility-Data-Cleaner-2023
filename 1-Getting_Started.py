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
string_vars = {'top_auths_by': 'Mentions', 'export_name': '', 'client_name': '','auth_skip_counter':0, 'auth_outlet_skipped':0}
for key, value in string_vars.items():
    if key not in st.session_state:
        st.session_state[key] = value
# 'page': '1: Getting Started',

df_vars = ['df_traditional', 'df_social', 'df_dupes', 'original_trad_auths', 'auth_outlet_table', 'original_auths',
           'df_untouched', 'author_outlets', 'blank_set', 'added_df', 'markdown_content',
           'filtered_df', 'df_grouped', 'selected_df', 'selected_rows', 'top_stories', 'auth_outlet_todo']
for _ in df_vars:
    if _ not in st.session_state:
        st.session_state[_] = pd.DataFrame()

step_vars = ['upload_step', 'standard_step', 'translated_headline', 'translated_summary', 'translated_snippet', 'filled', 'pickle_load']
for _ in step_vars:
    if _ not in st.session_state:
        st.session_state[_] = False

# counter_vars = ['counter', 'auth_outlet_skipped']
# for _ in counter_vars:
#     if _ not in st.session_state:
#         st.session_state[_] = 0

text_vars = ['ave_col']
for _ in text_vars:
    if _ not in st.session_state:
        st.session_state[_] = 'AVE'

if st.session_state.upload_step:
    st.success('File uploaded.')
    if st.button('Start Over?'):
        for key in st.session_state.keys():
            del st.session_state[key]
        st.rerun()

    # st.write(st.session_state.ave_col)
    st.session_state.df_untouched["Mentions"] = 1

    if "Impressions" in st.session_state.df_untouched:
        st.session_state.df_untouched = st.session_state.df_traditional.rename(columns={
            'Impressions': 'Audience Reach'})

    st.session_state.df_untouched['Audience Reach'] = st.session_state.df_untouched['Audience Reach'].astype('Int64')

    st.header('Quick View Stats')

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="Mentions", value="{:,}".format(len(st.session_state.df_untouched.dropna(thresh=3))))
        st.metric(label="Impressions", value="{:,}".format(st.session_state.df_untouched['Audience Reach'].sum()))
    with col2:
        st.subheader("Media Type")
        st.write(st.session_state.df_untouched['Media Type'].value_counts())
    with col3:
        st.subheader("Top Authors")
        original_top_authors = (mig.top_x_by_mentions(st.session_state.df_untouched, "Author"))
        st.write(original_top_authors)
        st.session_state.original_auths = original_top_authors
    with col4:
        st.subheader("Top Outlets")
        original_top_outlets = (mig.top_x_by_mentions(st.session_state.df_untouched, "Outlet"))
        st.write(original_top_outlets)

    df = st.session_state.df_untouched
    summary_stats = df.groupby("Published Date").agg({"Published Date": "count", "Audience Reach": "sum"})
    summary_stats.rename(columns={"Published Date": "Mentions", "Audience Reach": "Impressions"}, inplace=True)
    summary_stats.index.name = "Published Date"
    summary_stats.reset_index(inplace=True)

    col1, col2 = st.columns(2, gap="large")
    with col1:
        st.subheader('Mention Trend')
        st.area_chart(data=summary_stats, x="Published Date", y="Mentions", width=0, height=250, use_container_width=True)
    with col2:
        st.subheader('Impressions Trend')
        st.area_chart(data=summary_stats, x="Published Date", y="Impressions", width=0, height=250, use_container_width=True)

if not st.session_state.upload_step:
    with st.form('my_form'):
        client = st.text_input('Client organization name*', placeholder='eg. Air Canada', key='client',
                               help='Required to build export file name.')
        period = st.text_input('Reporting period or focus*', placeholder='eg. March 2022', key='period',
                               help='Required to build export file name.')
        uploaded_file = st.file_uploader(label='Upload your CSV*', type='csv',
                                         accept_multiple_files=False,
                                         help='Only use CSV files exported from the Agility Platform.')

        submitted = st.form_submit_button("Submit", type="primary")
        if submitted and (client == "" or period == "" or uploaded_file is None):
            st.error('Missing required form inputs above.')

        elif submitted:
            with st.spinner("Converting file format."):
                st.session_state.df_untouched = pd.read_csv(uploaded_file)
                if "Impressions" in st.session_state.df_untouched:
                    st.session_state.df_untouched = st.session_state.df_untouched.rename(columns={
                        'Impressions': 'Audience Reach'})

                st.session_state.df_untouched = st.session_state.df_untouched.dropna(thresh=3)
                st.session_state.df_untouched["Mentions"] = 1

                st.session_state.df_untouched['Audience Reach'] = st.session_state.df_untouched['Audience Reach'].astype('Int64')

                # DEALING WITH VARIABLE AVE NAMES
                # assume the original DataFrame is stored in a variable called `df`
                # find the column with the "AVE" and currency type in parentheses, or just "AVE"
                st.session_state.ave_col = [col for col in st.session_state.df_untouched.columns if
                                            re.match(r'^AVE\([A-Z]{2,3}\)$', col)] or [col for col in
                                                                                       st.session_state.df_untouched.columns
                                                                                       if col == "AVE"]

                # rename the column to "AVE" for data cleaning
                st.session_state.df_untouched = st.session_state.df_untouched.rename(columns={st.session_state.ave_col[0]: 'AVE'})

                st.session_state.df_untouched['AVE'] = st.session_state.df_untouched['AVE'].fillna(0)
                st.session_state.export_name = f"{client} - {period}"
                st.session_state.client_name = client

                st.session_state.df_traditional = st.session_state.df_untouched
                st.session_state.df_traditional.drop(["Timezone",
                                              "Word Count",
                                              "Duration",
                                              "Image URLs",
                                              "Folders",
                                              "Notes",
                                              "County"],
                                                     axis=1, inplace=True, errors='ignore')

                st.session_state.df_traditional = st.session_state.df_traditional.astype(
                    {
                     "Sentiment": 'category',
                     "Continent": 'category',
                     "Country": 'category',
                     "Province/State": 'category',
                     "City": 'category',
                     "Language": 'category'
                     })

                if "Published Date" in st.session_state.df_traditional:
                    st.session_state.df_traditional['Date'] = pd.to_datetime(st.session_state.df_traditional['Published Date'] + ' ' + st.session_state.df_traditional['Published Time'])
                    st.session_state.df_traditional.drop(["Published Date", "Published Time"], axis=1, inplace=True, errors='ignore')

                    first_column = st.session_state.df_traditional.pop('Date')
                    st.session_state.df_traditional.insert(0, 'Date', first_column)

                st.session_state.df_traditional = st.session_state.df_traditional.rename(columns={
                    'Media Type': 'Type',
                    'Coverage Snippet': 'Snippet',
                    'Province/State': 'Prov/State',
                    'Audience Reach': 'Impressions'})

                st.session_state.upload_step = True
                st.rerun()