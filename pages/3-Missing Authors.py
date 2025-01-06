import streamlit as st
import pandas as pd
import mig_functions as mig
import warnings
import urllib.parse
import numpy as np




warnings.filterwarnings('ignore')
st.set_page_config(layout="wide", page_title="MIG Data Processing App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")

mig.standard_sidebar()

st.title('Authors - Missing')

if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')
elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')

elif len(st.session_state.df_traditional) == 0:
    st.subheader("No traditional media in data. Skip to next step.")

else:
    counter = st.session_state.auth_skip_counter
    reviewed = st.session_state.get('auth_reviewed_count', 0)  # Initialize reviewed count if not present


    # CSS to inject contained in a string
    hide_table_row_index = """
                        <style>
                        tbody th {display:none}
                        .blank {display:none}
                        </style>
                        """
    # Inject CSS with Markdown
    st.markdown(hide_table_row_index, unsafe_allow_html=True)

    # Replace blank strings in the 'Author' column with NaN
    st.session_state.df_traditional['Author'].replace('', np.nan, inplace=True)

    headline_table = st.session_state.df_traditional[['Headline', 'Mentions', 'Author']]
    headline_table = headline_table.groupby("Headline").count()
    headline_table["Missing"] = headline_table["Mentions"] - headline_table["Author"]
    headline_table = headline_table[(headline_table["Author"] > 0) & (headline_table['Missing'] > 0)].sort_values(
        "Missing", ascending=False).reset_index()
    headline_table.rename(columns={'Author': 'Known', 'Mentions': 'Total'},
                          inplace=True, errors='raise')

    temp_headline_list = headline_table
    if counter < len(temp_headline_list):
        headline_text = temp_headline_list.iloc[counter]['Headline']
        # Encode the headline to handle spaces and special characters
        encoded_headline = urllib.parse.quote(f'"{headline_text}"')  # Quotes added for exact match search

        # Create the Google search URL
        google_search_url = f"https://www.google.com/search?q={encoded_headline}"


        but1, col3, but2 = st.columns(3)
        with but1:
            next_auth = st.button('Skip to Next Headline')
            if next_auth:
                counter += 1
                st.session_state.auth_skip_counter = counter
                st.rerun()

        if counter > 0:
            with col3:
                st.write(f"Skipped: {counter}")
            with but2:
                reset_counter = st.button('Reset Skip Counter')
                if reset_counter:
                    counter = 0
                    st.session_state.auth_skip_counter = counter
                    st.rerun()

        possibles = mig.headline_authors(st.session_state.df_traditional, headline_text)

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
                if len(mig.headline_authors(st.session_state.df_traditional, headline_text)) > 5:
                    st.table(mig.headline_authors(st.session_state.df_traditional, headline_text).rename(
                        columns={'index': 'Possible Author(s)', 'Author': 'Matches'}))
                else:
                    # CHANGED from st.dataframe to table to hide index on first load.  Other consequences?
                    st.table(mig.headline_authors(st.session_state.df_traditional, headline_text).rename(
                        columns={'index': 'Possible Author(s)',
                                 'Author': 'Matches'}))

        with form_block:
            with st.form('auth updater', clear_on_submit=True):

                col1, col2, col3 = st.columns([8, 1, 8])
                with col1:
                    box_author = st.selectbox('Pick from possible Authors', possibles,
                                              help='Pick from one of the authors already associated with this headline.')

                with col2:
                    st.write(" ")
                    st.subheader("OR")

                with col3:
                    string_author = st.text_input("Write in the author name",
                                                  help='Override above selection by writing in a custom name.')

                if len(string_author) > 0:
                    new_author = string_author
                else:
                    new_author = box_author

                submitted = st.form_submit_button("Update Author", type="primary")
                if submitted:
                    mig.fix_author(st.session_state.df_traditional, headline_text, new_author)

                    # Increment the counter to reflect the headline being reviewed
                    # Increment reviewed count and counter
                    reviewed += 1
                    st.session_state['auth_reviewed_count'] = reviewed

                    # counter += 1
                    # st.session_state.auth_skip_counter = counter

                    st.rerun()
    else:
        st.info("You've reached the end of the list!")
        if counter > 0:
            reset_counter = st.button('Reset Counter')
            if reset_counter:
                counter = 0
                st.session_state.auth_skip_counter = counter
                st.rerun()
        else:
            st.success("✓ Nothing left to update here.")



    def fixable_headline_stats(df, primary="Headline", secondary="Author"):
        """tells you how many author fields can be fixed and other stats"""
        total = df["Mentions"].count()
        headline_table = pd.pivot_table(df, index=primary, values=["Mentions", secondary], aggfunc="count")
        headline_table["Missing"] = headline_table["Mentions"] - headline_table[secondary]
        missing = headline_table.Missing.sum()
        headline_table = headline_table[headline_table[secondary] > 0]
        headline_table = headline_table[headline_table['Missing'] > 0]
        fixable = headline_table.Missing.sum()
        fixable_headline_count = headline_table.Missing.count()
        remaining = fixable_headline_count - counter
        total_known = total - missing
        percent_known = "{:.0%}".format((total_known) / total)
        percent_knowable = "{:.0%}".format((total - (missing - fixable)) / total)
        stats = {
            "total": total,
            "total_known": total_known,
            "percent_known": percent_known,
            "fixable": fixable,
            "fixable_headline_count": fixable_headline_count,
            "remaining": remaining,
            "percent_knowable": percent_knowable
        }
            # (
            # f"Total rows: \t\t{total} \nTotal Known: \t\t{total_known}\nPercent Known: \t\t{percent_known} \nFixable Fields: \t{fixable}\nUnique Fixable: \t{fixable_headline_count}\nPercent knowable: \t{percent_knowable}")
        return stats

    st.divider()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("Original Top Authors")
        media_type_column = "Type" if "Type" in st.session_state.df_untouched.columns else "Media Type"

        filtered_df = st.session_state.df_untouched[
            st.session_state.df_untouched[media_type_column].isin(['PRINT', 'ONLINE_NEWS', 'ONLINE', 'BLOGS', 'PRESS_RELEASE'])]

        original_top_authors = (mig.top_x_by_mentions(filtered_df, "Author"))
        st.write(original_top_authors)


    with col2:
        st.subheader("New Top Authors")
        st.dataframe((mig.top_x_by_mentions(st.session_state.df_traditional, "Author")))

    with col3:
        st.subheader("Fixable Author Stats")
        remaining = (fixable_headline_stats(st.session_state.df_traditional, primary="Headline", secondary="Author"))

        statscol1, statscol2 = st.columns(2)

        with statscol1:
            st.metric(label="Updated", value=reviewed)
            # st.metric(label="Total Rows", value=remaining['total'])
            st.metric(label="Percent Known", value=remaining['percent_known'])

        with statscol2:
            st.metric(label="Not Updated", value=len(temp_headline_list) - reviewed)
            # st.metric(label="Unreviewed", value=remaining['remaining'])
            # st.metric(label="Total Known", value=remaining['total_known'])
            st.metric(label="Percent Knowable", value=remaining['percent_knowable'])

        # st.write(remaining)



