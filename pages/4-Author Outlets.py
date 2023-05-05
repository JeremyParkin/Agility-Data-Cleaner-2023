import streamlit as st
import pandas as pd
import mig_functions as mig
from unidecode import unidecode
import requests
from requests.structures import CaseInsensitiveDict
import warnings
warnings.filterwarnings('ignore')
st.set_page_config(layout="wide", page_title="MIG Data Cleaning App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")

mig.standard_sidebar()

format_dict = {'AVE': '${0:,.0f}', 'Audience Reach': '{:,d}', 'Impressions': '{:,d}'}

st.title("Author - Outlets")
if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')
elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')
else:

    def name_match(series):
        non_match = 'color: #985331;'
        match = 'color: goldenrod'
        return [non_match if cell_value != author_name else match for cell_value in series]

    def reset_skips():
        st.session_state.auth_outlet_skipped = 0

    def fetch_outlet(author_name):
        contact_url = "https://mediadatabase.agilitypr.com/api/v4/contacts/search"
        headers = CaseInsensitiveDict()
        headers["Content-Type"] = "text/json"
        headers["Accept"] = "text/json"
        headers["Authorization"] = st.secrets["authorization"]
        headers["client_id"] = st.secrets["client_id"]
        headers["userclient_id"] = st.secrets["userclient_id"]

        data_a = '''
      {  
        "aliases": [  
          "'''

        data_b = '''"  
        ]   
      }
      '''

        data = data_a + author_name + data_b
        contact_resp = requests.post(contact_url, headers=headers, data=data)

        return contact_resp.json()


    st.session_state.df_traditional.Mentions = st.session_state.df_traditional.Mentions.astype('int')
    auth_outlet_skipped = st.session_state.auth_outlet_skipped
    auth_outlet_table = st.session_state.auth_outlet_table

    st.session_state.top_auths_by = st.selectbox('Top Authors by: ', ['Mentions', 'Impressions'], on_change=reset_skips)
    if len(auth_outlet_table) == 0:
        if st.session_state.top_auths_by == 'Mentions':
            auth_outlet_table = st.session_state.df_traditional[['Author', 'Mentions', 'Impressions']].groupby(
                by=['Author']).sum().sort_values(
                ['Mentions', 'Impressions'], ascending=False).reset_index()

        elif st.session_state.top_auths_by == 'Impressions':
            auth_outlet_table = st.session_state.df_traditional[['Author', 'Mentions', 'Impressions']].groupby(
                by=['Author']).sum().sort_values(
                ['Impressions', 'Mentions'], ascending=False).reset_index()

        # auth_outlet_table['Outlet'] = ''
        auth_outlet_table.insert(loc=1, column='Outlet', value='')

        auth_outlet_todo = auth_outlet_table

    else:
        if st.session_state.top_auths_by == 'Mentions':
            auth_outlet_table = auth_outlet_table.sort_values(['Mentions', 'Impressions'],
                                                              ascending=False)  # .reset_index()

        elif st.session_state.top_auths_by == 'Impressions':
            auth_outlet_table = auth_outlet_table.sort_values(['Impressions', 'Mentions'],
                                                              ascending=False)  # .reset_index()

        auth_outlet_todo = auth_outlet_table.loc[auth_outlet_table['Outlet'] == '']

    auth_outlet_skipped = st.session_state.auth_outlet_skipped

    if auth_outlet_skipped < len(auth_outlet_todo):
        author_name = auth_outlet_todo.iloc[auth_outlet_skipped]['Author']

        # NAME, SKIP & RESET SKIP SECTION
        col1, but1, but2 = st.columns([2, 1, 1])
        with col1:
            st.markdown("""
                            <h2 style="color: goldenrod; padding-top:0!important; margin-top:-"> 
                            """ + author_name +
                        """</h2>
                        <style>.css-12w0qpk {padding-top:22px !important}</style>
                        """, unsafe_allow_html=True)
        with but1:
            next_auth = st.button('Skip to Next Author')
            if next_auth:
                auth_outlet_skipped += 1
                st.session_state.auth_outlet_skipped = auth_outlet_skipped
                st.experimental_rerun()

            with but2:
                reset_counter = st.button('Reset Skips')
                if reset_counter:
                    auth_outlet_skipped = 0
                    st.session_state.auth_outlet_skipped = auth_outlet_skipped
                    st.experimental_rerun()

        search_results = fetch_outlet(unidecode(author_name))
        db_outlets = []

        if 'results' in search_results:
            number_of_authors = len(search_results['results'])

            if not search_results['results']:
                matched_authors = []
            elif search_results['results'] is None:
                matched_authors = []
            else:
                response_results = search_results['results']
                outlet_results = []

                for result in response_results:
                    auth_name = result['firstName'] + " " + result['lastName']
                    job_title = result['primaryEmployment']['jobTitle']
                    outlet = result['primaryEmployment']['outletName']
                    if result['country'] is None:
                        country = ''

                    else:
                        country = result['country']['name']
                    auth_tuple = (auth_name, job_title, outlet, country)
                    outlet_results.append(auth_tuple)

                matched_authors = pd.DataFrame.from_records(outlet_results,
                                                            columns=['Name', 'Title', 'Outlet', 'Country'])
                matched_authors.loc[matched_authors.Outlet == "[Freelancer]", "Outlet"] = "Freelance"

                db_outlets = matched_authors.Outlet.tolist()

        # OUTLETS IN COVERAGE VS DATABASE
        # CSS to inject contained in a string
        hide_table_row_index = """
                                        <style>
                                        tbody th {display:none}
                                        .blank {display:none}
                                        </style>
                                        """

        hide_dataframe_row_index = """
                    <style>
                    .row_heading.level0 {width:0; display:none}
                    .blank {width:0; display:none}
                    </style>
                    """

        # Inject CSS with Markdown
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)

        form_block = st.container()
        info_block = st.container()

        with info_block:
            col1, col2, col3 = st.columns([8, 1, 16])
            with col1:
                st.subheader("Outlets in CSV")
                outlets_in_coverage = st.session_state.df_traditional.loc[
                    st.session_state.df_traditional.Author == author_name].Outlet.value_counts()
                outlets_in_coverage_list = outlets_in_coverage.index
                outlets_in_coverage_list = outlets_in_coverage_list.insert(0, "Freelance")
                outlets_in_coverage = outlets_in_coverage.rename_axis('Outlet').reset_index(name='Hits')

                if len(outlets_in_coverage) > 7:
                    st.dataframe(outlets_in_coverage.style.apply(lambda x: [
                        'color: goldenrod' if v in db_outlets else "" for v in x],
                        axis=1, subset="Outlet"))
                else:
                    st.table(outlets_in_coverage.style.apply(lambda x: [
                        'color: goldenrod' if v in db_outlets else "" for v in x],
                        axis=1, subset="Outlet"))

            with col2:
                st.write(" ")

            with col3:
                st.subheader("Media Database Results")
                if 'results' not in search_results:
                    st.warning("NO MATCH FOUND")
                    matched_authors = []
                elif not search_results['results']:
                    st.warning("NO MATCH FOUND")
                    matched_authors = []
                elif search_results is None:
                    st.warning("NO MATCH FOUND")
                    matched_authors = []

                else:
                    response_results = search_results['results']
                    outlet_results = []

                    for result in response_results:
                        auth_name = result['firstName'] + " " + result['lastName']
                        job_title = result['primaryEmployment']['jobTitle']
                        outlet = result['primaryEmployment']['outletName']
                        if result['country'] is None:
                            country = 'None'

                        else:
                            country = result['country']['name']
                        auth_tuple = (auth_name, job_title, outlet, country)
                        outlet_results.append(auth_tuple)

                    matched_authors = pd.DataFrame.from_records(outlet_results,
                                                                columns=['Name', 'Title', 'Outlet', 'Country'])
                    matched_authors.loc[matched_authors.Outlet == "[Freelancer]", "Outlet"] = "Freelance"

                    # Check if any outlets in `matched_authors` match `outlets_in_coverage_list`
                    matching_outlets = set(outlets_in_coverage_list).intersection(set(matched_authors.Outlet.tolist()))

                    possibles = matched_authors.Outlet.tolist()

                    # If a matching outlet is found and there are multiple entries in `possibles`, move the matched outlet to the top
                    if len(matching_outlets) > 0 and len(possibles) > 1:
                        matched_authors_top = matched_authors[matched_authors.Outlet.isin(matching_outlets)]
                        matched_authors_bottom = matched_authors[~matched_authors.Outlet.isin(matching_outlets)]
                        matched_authors = pd.concat([matched_authors_top, matched_authors_bottom])

                    if len(matched_authors) > 7:
                        st.dataframe(matched_authors.style.apply(lambda x: [
                            'background: goldenrod; color: black' if v in outlets_in_coverage.Outlet.tolist() else ""
                            for v in x], axis=1).apply(name_match, axis=0, subset='Name'))

                    else:
                        st.table(matched_authors.style.apply(lambda x: [
                            'background: goldenrod; color: black' if v in outlets_in_coverage.Outlet.tolist() else ""
                            for v in x], axis=1).apply(name_match, axis=0, subset='Name'))

                    # Check if there is a match between outlets_in_coverage_list and possibles
                    matching_outlet = [outlet for outlet in outlets_in_coverage_list if outlet in possibles]

                    if len(matching_outlet) == 1:
                        # Get the index of the matching element in possibles
                        index = possibles.index(matching_outlet[0])
                        # Move the matching element to the first place
                        possibles = [matching_outlet[0]] + possibles[:index] + possibles[index + 1:]

        with form_block:
            # FORM TO UPDATE AUTHOR OUTLET ######################
            with st.form('auth updater', clear_on_submit=True):
                col1, col2, col3 = st.columns([8, 1, 8])
                with col1:
                    if len(matched_authors) > 0:
                        box_outlet = st.selectbox('Pick outlet from DATABASE MATCHES', possibles,
                                                  help='Pick from one of the outlets associated with this author name.')

                    else:
                        box_outlet = st.selectbox('Pick outlet from COVERAGE or "Freelance"', outlets_in_coverage_list)

                with col2:
                    st.write(" ")
                    st.subheader("OR")
                with col3:
                    string_outlet = st.text_input("Write in an outlet name",
                                                  help='Override above selection by writing in a custom name.')

                submitted = st.form_submit_button("Assign Outlet")

        if submitted:
            if len(string_outlet) > 0:
                new_outlet = string_outlet
            else:
                new_outlet = box_outlet

            auth_outlet_table.loc[auth_outlet_table["Author"] == author_name, "Outlet"] = new_outlet
            st.session_state.auth_outlet_skipped = auth_outlet_skipped
            st.session_state.auth_outlet_table = auth_outlet_table
            st.experimental_rerun()

        st.divider()
        col1, col2, col3 = st.columns([8, 1, 4])
        with col1:
            st.subheader("Top Authors")
            if 'Outlet' in auth_outlet_table.columns:
                if st.session_state.top_auths_by == 'Mentions':
                    st.table(
                        auth_outlet_table[['Author', 'Outlet', 'Mentions', 'Impressions']].fillna('').sort_values(
                            ['Mentions', 'Impressions'], ascending=False).head(15).style.format(
                            format_dict, na_rep=' '))
                if st.session_state.top_auths_by == 'Impressions':
                    st.table(
                        auth_outlet_table[['Author', 'Outlet', 'Mentions', 'Impressions']].fillna('').sort_values(
                            ['Impressions', 'Mentions'], ascending=False).head(15).style.format(
                            format_dict, na_rep=' '))

            else:
                st.table(auth_outlet_table[['Author', 'Mentions', 'Impressions']].fillna('').head(15).style.format(
                    format_dict, na_rep=' '))
        with col2:
            st.write(" ")
        with col3:
            st.subheader('Outlets assigned')

            if 'Outlet' in auth_outlet_table.columns:
                assigned = len(auth_outlet_table.loc[auth_outlet_table['Outlet'] != ''])
                st.metric(label='Assigned', value=assigned)
            else:
                st.metric(label='Assigned', value=0)
    else:
        st.info("You've reached the end of the list!")
        st.write(f"Authors skipped: {auth_outlet_skipped}")
        if auth_outlet_skipped > 0:
            reset_counter = st.button('Reset Counter')
            if reset_counter:
                auth_outlet_skipped = 0
                st.session_state.auth_outlet_skipped = auth_outlet_skipped
                st.experimental_rerun()
        else:
            st.write("✓ Nothing left to update here.")
