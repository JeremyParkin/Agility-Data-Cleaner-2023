import streamlit as st
import pandas as pd
import mig_functions as mig
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from titlecase import titlecase
import warnings
warnings.filterwarnings('ignore')
st.set_page_config(layout="wide", page_title="MIG Data Cleaning App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")


mig.standard_sidebar()


st.title('Translation')


traditional = st.session_state.df_traditional
if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')
elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')
# elif st.session_state.translated_headline == True and st.session_state.translated_snippet == True and
#   st.session_state.translated_summary == True:
elif st.session_state.translated_headline and st.session_state.translated_snippet and st.session_state.translated_summary:
    st.subheader("✓ Translation complete.")
    trad_non_eng = len(traditional[traditional['Language'] != 'English'])
    soc_non_eng = len(st.session_state.df_social[st.session_state.df_social['Language'] != 'English'])

    if trad_non_eng > 0:
        with st.expander("Traditional - Non-English"):
            st.dataframe(traditional[traditional['Language'] != 'English'][
                             ['Outlet', 'Headline', 'Snippet', 'Contextual Snippet', 'Language', 'Country']])

    if soc_non_eng > 0:
        with st.expander("Social - Non-English"):
            st.dataframe(st.session_state.df_social[st.session_state.df_social['Language'] != 'English'][
                             ['Outlet', 'Snippet', 'Contextual Snippet', 'Language', 'Country']])
elif len(traditional[traditional['Language'] != 'English']) == 0 and len(
        st.session_state.df_social[st.session_state.df_social['Language'] != 'English']) == 0:
    st.subheader("No translation required")
else:
    def translate_col(df, name_of_column):
        """Replaces non-English string in column with English"""
        global dictionary
        dictionary = {}
        unique_non_eng = list(set(df[name_of_column][df['Language'] != 'English'].dropna()))
        if '' in unique_non_eng:
            unique_non_eng.remove('')
        with st.spinner('Running translation now...'):
            with ThreadPoolExecutor(max_workers=30) as ex:
                results = ex.map(translate, [text for text in unique_non_eng])
        df[name_of_column].replace(dictionary, inplace=True)


    def translate(text):
        dictionary[text] = (GoogleTranslator(source='auto', target='en').translate(text[:1500]))


    def translation_stats_combo():
        non_english_records = len(traditional[traditional['Language'] != 'English']) + len(
            st.session_state.df_social[st.session_state.df_social['Language'] != 'English'])

        st.write(f"There are {non_english_records} non-English records in your data.")


    translation_stats_combo()
    if len(traditional) > 0:
        with st.expander("Traditional - Non-English"):
            st.dataframe(traditional[traditional['Language'] != 'English'][
                             ['Outlet', 'Headline', 'Snippet', 'Contextual Snippet', 'Language', 'Country']])

    if len(st.session_state.df_social) > 0:
        with st.expander("Social - Non-English"):
            st.dataframe(st.session_state.df_social[st.session_state.df_social['Language'] != 'English'][
                             ['Outlet', 'Snippet', 'Contextual Snippet', 'Language', 'Country']])

    with st.form('translation_form'):
        st.subheader("Pick columns for translations")
        st.warning("WARNING: Translation will over-write the original text.")

        if len(traditional) > 0:
            if not st.session_state.translated_headline:
                headline_to_english = st.checkbox('Headline', value=True)
            else:
                st.success('✓ Headlines translated.')
                headline_to_english = False
        else:
            headline_to_english = False

        if not st.session_state.translated_snippet:
            snippet_to_english = st.checkbox('Snippet', value=True)
        else:
            st.success('✓ Snippets translated.')
            snippet_to_english = False

        if not st.session_state.translated_summary:
            summary_to_english = st.checkbox('Contextual Snippet', value=True)
        else:
            st.success('✓ Summaries translated.')
            summary_to_english = False

        submitted = st.form_submit_button("Go!", type="primary")
        if submitted:
            st.warning("Stay on this page until translation is complete")

            if headline_to_english:
                traditional['Original Headline'] = traditional.Headline
                translate_col(traditional, 'Headline')

                # AP Cap
                broadcast_array = ['RADIO', 'TV']
                broadcast = traditional.loc[traditional['Type'].isin(broadcast_array)]
                traditional = traditional[~traditional['Type'].isin(broadcast_array)]
                traditional[['Headline']] = traditional[['Headline']].fillna('')
                traditional['Headline'] = traditional['Headline'].map(lambda Headline: titlecase(Headline))
                frames = [traditional, broadcast]
                traditional = pd.concat(frames)

                st.session_state.df_social['Original Headline'] = st.session_state.df_social.Headline

                translate_col(st.session_state.df_social, 'Headline')
                st.session_state.translated_headline = True
                st.success(f'Done translating headlines!')
            if summary_to_english:
                translate_col(traditional, 'Contextual Snippet')
                translate_col(st.session_state.df_social, 'Contextual Snippet')
                st.session_state.translated_summary = True
                st.success(f'Done translating summaries!')
            if snippet_to_english:
                translate_col(traditional, 'Snippet')
                translate_col(st.session_state.df_social, 'Snippet')
                st.session_state.translated_snippet = True
                st.success(f'Done translating snippets!')
            st.session_state.df_traditional = traditional
            st.experimental_rerun()
