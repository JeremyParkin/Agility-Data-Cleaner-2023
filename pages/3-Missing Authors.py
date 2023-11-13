import streamlit as st
import pandas as pd
import mig_functions as mig
import warnings
warnings.filterwarnings('ignore')
st.set_page_config(layout="wide", page_title="MIG Data Cleaning App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")

mig.standard_sidebar()

st.title('Authors - Missing')
# original_trad_auths = st.session_state.original_trad_auths

if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')

elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')
elif len(st.session_state.df_traditional) == 0:
    st.subheader("No traditional media in data. Skip to next step.")

else:
    counter = st.session_state.counter
    original_top_authors = st.session_state.original_auths

    # CSS to inject contained in a string
    hide_table_row_index = """
                        <style>
                        tbody th {display:none}
                        .blank {display:none}
                        </style>
                        """
    # Inject CSS with Markdown
    st.markdown(hide_table_row_index, unsafe_allow_html=True)

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

        but1, col3, but2 = st.columns(3)
        with but1:
            next_auth = st.button('Skip to Next Headline')
            if next_auth:
                counter += 1
                st.session_state.counter = counter
                st.experimental_rerun()

        if counter > 0:
            with col3:
                st.write(f"Skipped: {counter}")
            with but2:
                reset_counter = st.button('Reset Skip Counter')
                if reset_counter:
                    counter = 0
                    st.session_state.counter = counter
                    st.experimental_rerun()

        possibles = mig.headline_authors(st.session_state.df_traditional, headline_text)

        form_block = st.container()
        info_block = st.container()

        with info_block:
            col1, col2, col3 = st.columns([12, 1, 9])
            with col1:
                st.subheader("Headline")
                st.table(headline_table.iloc[[counter]])
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
                    st.experimental_rerun()
    else:
        st.info("You've reached the end of the list!")
        if counter > 0:
            reset_counter = st.button('Reset Counter')
            if reset_counter:
                counter = 0
                st.session_state.counter = counter
                st.experimental_rerun()
        else:
            st.success("✓ Nothing left to update here.")


    def fixable_headline_stats(df, primary="Headline", secondary="Author"):
        """tells you how many author fields can be fixed and other stats"""
        headline_table = pd.pivot_table(df, index=primary, values=["Mentions", secondary], aggfunc="count")
        headline_table["Missing"] = headline_table["Mentions"] - headline_table[secondary]
        headline_table = headline_table[headline_table[secondary] > 0]
        headline_table = headline_table[headline_table['Missing'] > 0]
        fixable_headline_count = headline_table.Missing.count()
        remaining = fixable_headline_count - counter
        return remaining

    st.divider()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("Original Top Authors")
        st.dataframe(st.session_state.original_trad_auths)

    with col2:
        st.subheader("New Top Authors")
        st.dataframe((mig.top_x_by_mentions(st.session_state.df_traditional, "Author")))
    with col3:
        st.subheader("Fixable Author Stats")
        remaining = (fixable_headline_stats(st.session_state.df_traditional, primary="Headline", secondary="Author"))
        # st.text(stats)
        st.metric(label="Remaining to Review", value=remaining)
