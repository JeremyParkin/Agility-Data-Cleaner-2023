import streamlit as st
import pandas as pd
import mig_functions as mig
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from titlecase import titlecase
import warnings

warnings.filterwarnings('ignore')
st.set_page_config(layout="wide", page_title="MIG Data Processing App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")


mig.standard_sidebar()


st.title('Translation')


def display_non_english_records(df, title):
    if any(df['Language'] != 'English'):
        with st.expander(f"{title} - Non-English"):
            st.dataframe(df[df['Language'] != 'English'][['Outlet', 'Headline', 'Snippet', 'Contextual Snippet', 'Language', 'Country']])

# Initialize dataframes if empty
columns_to_add = ['Headline', 'Snippet', 'Contextual Snippet', 'Language']
st.session_state.df_traditional = st.session_state.df_traditional if len(st.session_state.df_traditional) else pd.DataFrame(columns=columns_to_add)
st.session_state.df_social = st.session_state.df_social if len(st.session_state.df_social) else pd.DataFrame(columns=columns_to_add)


def count_non_english_records(df):
    if len(df) > 0:
        return len(df[df['Language'] != 'English'])
    return 0



def translate_col(df, name_of_column):
    """Replaces non-English string in column with English"""
    if name_of_column in df.columns:
        global dictionary
        dictionary = {}
        unique_non_eng = list(set(df[name_of_column][df['Language'] != 'English'].dropna()))
        if '' in unique_non_eng:
            unique_non_eng.remove('')
        with st.spinner('Running translation now...'):
            with ThreadPoolExecutor(max_workers=30) as ex:
                results = ex.map(translate, [text for text in unique_non_eng])
        df[name_of_column].replace(dictionary, inplace=True)



trad_non_eng = count_non_english_records(st.session_state.df_traditional)
soc_non_eng = count_non_english_records(st.session_state.df_social)

if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')
elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')
# elif st.session_state.translated_headline == True and st.session_state.translated_snippet == True and
#   st.session_state.translated_summary == True:
elif st.session_state.translated_headline and st.session_state.translated_snippet and st.session_state.translated_summary:
    st.subheader("✓ Translation complete.")

    display_non_english_records(st.session_state.df_traditional, "Traditional")
    display_non_english_records(st.session_state.df_social, "Social")


elif trad_non_eng + soc_non_eng == 0:
    st.subheader("No translation required")

else:
    def translate(text):
        dictionary[text] = (GoogleTranslator(source='auto', target='en').translate(text[:2000]))


    def translation_stats_combo():
        non_english_records = soc_non_eng + trad_non_eng
        st.write(f"There are {non_english_records} non-English records in your data.")


    translation_stats_combo()


    display_non_english_records(st.session_state.df_traditional, "Traditional")
    display_non_english_records(st.session_state.df_social, "Social")

    with st.form('translation_form'):
        st.subheader("Pick columns for translations")
        st.warning("WARNING: Translation will over-write the original text.")

        headline_to_english = st.checkbox('Headline', value=True, disabled=st.session_state.translated_headline)
        snippet_to_english = st.checkbox('Snippet', value=True, disabled=st.session_state.translated_snippet)
        summary_to_english = st.checkbox('Contextual Snippet', value=True, disabled=st.session_state.translated_summary)


        if st.form_submit_button("Go!", type="primary"):
            st.warning("Stay on this page until translation is complete")

            if headline_to_english:
                st.session_state.df_traditional['Original Headline'] = st.session_state.df_traditional.Headline
                translate_col(st.session_state.df_traditional, 'Headline')

                # AP Cap
                broadcast_array = ['RADIO', 'TV']
                broadcast = st.session_state.df_traditional.loc[st.session_state.df_traditional['Type'].isin(broadcast_array)]
                st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional['Type'].isin(broadcast_array)]
                st.session_state.df_traditional[['Headline']] = st.session_state.df_traditional[['Headline']].fillna('')
                st.session_state.df_traditional['Headline'] = st.session_state.df_traditional['Headline'].map(lambda Headline: titlecase(Headline))
                frames = [st.session_state.df_traditional, broadcast]
                st.session_state.df_traditional = pd.concat(frames)

                st.session_state.df_social['Original Headline'] = st.session_state.df_social.Headline

                translate_col(st.session_state.df_social, 'Headline')
                st.session_state.translated_headline = True
                st.success(f'Done translating headlines!')
            if summary_to_english:
                translate_col(st.session_state.df_traditional, 'Contextual Snippet')
                translate_col(st.session_state.df_social, 'Contextual Snippet')
                st.session_state.translated_summary = True
                st.success(f'Done translating summaries!')
            if snippet_to_english:
                translate_col(st.session_state.df_traditional, 'Snippet')
                translate_col(st.session_state.df_social, 'Snippet')
                st.session_state.translated_snippet = True
                st.success(f'Done translating snippets!')
            st.rerun()
