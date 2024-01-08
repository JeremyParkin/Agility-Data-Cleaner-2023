import sys

import streamlit as st
import pandas as pd
import mig_functions as mig
from titlecase import titlecase
import warnings
warnings.filterwarnings('ignore')
st.set_page_config(layout="wide", page_title="MIG Data Processing App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")


mig.standard_sidebar()

format_dict = {'AVE': '${0:,.0f}', 'Audience Reach': '{:,d}', 'Impressions': '{:,d}'}

st.title('Standard Cleaning')

# st.write(sys.getsizeof(st.session_state))
#
# st.write(st.session_state)


# impressions = st.session_state.df_untouched['Audience Reach'].sum()
# st.metric(label="Impressions", value=mig.format_number(impressions))



if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')
elif st.session_state.standard_step:
    st.success("Standard cleaning done!")

    if len(st.session_state.df_traditional) > 0:
        with st.expander("Traditional"):
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Basic Metrics")
                st.metric(label="Mentions", value="{:,}".format(len(st.session_state.df_traditional)))
                trad_impressions = st.session_state.df_traditional['Impressions'].sum()
                st.metric(label="Impressions", value=mig.format_number(trad_impressions))
                # st.metric(label="Impressions", value="{:,}".format(st.session_state.df_traditional['Impressions'].sum()))
            with col2:
                st.subheader("Media Type")
                st.write(st.session_state.df_traditional['Type'].value_counts())
            st.subheader("Data")
            st.markdown('(First 50 rows)')
            st.dataframe(st.session_state.df_traditional.head(50).style.format(format_dict, na_rep=' '))

    if len(st.session_state.df_social) > 0:
        with st.expander("Social"):
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Basic Metrics")
                st.metric(label="Mentions", value="{:,}".format(len(st.session_state.df_social)))
                soc_impressions = st.session_state.df_social['Impressions'].sum()
                st.metric(label="Impressions", value=mig.format_number(soc_impressions))
                # st.metric(label="Impressions", value="{:,}".format(st.session_state.df_social['Impressions'].sum()))
            with col2:
                st.subheader("Media Type")
            st.subheader("Data")
            st.markdown('(First 50 rows)')
            st.dataframe(st.session_state.df_social.head(50).style.format(format_dict, na_rep=' '))
    if len(st.session_state.df_dupes) > 0:
        with st.expander("Deleted Duplicates"):
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Basic Metrics")
                st.metric(label="Mentions", value="{:,}".format(len(st.session_state.df_dupes)))
                dup_impressions = st.session_state.df_dupes['Impressions'].sum()
                st.metric(label="Impressions", value=mig.format_number(dup_impressions))
                # st.metric(label="Impressions", value="{:,}".format(st.session_state.df_dupes['Impressions'].sum()))
            with col2:
                st.subheader("Media Type")
                st.write(st.session_state.df_dupes['Type'].value_counts())

            st.dataframe(st.session_state.df_dupes.style.format(format_dict, na_rep=' '))
else:
    with st.form("my_form_basic_cleaning"):
        st.subheader("Cleaning options")
        merge_online = st.checkbox("Merge 'blogs' and 'press releases' into 'Online'", value=True)
        drop_dupes = st.checkbox("Drop duplicates", value=True)

        submitted = st.form_submit_button("Go!", type="primary")
        if submitted:
            with st.spinner("Running standard cleaning."):

                st.session_state.df_traditional["Type"].replace({"ONLINE_NEWS": "ONLINE NEWS", "PRESS_RELEASE": "PRESS RELEASE"}, inplace=True)
                st.session_state.df_traditional.loc[st.session_state.df_traditional['URL'].str.contains("www.facebook.com", na=False), 'Type'] = "FACEBOOK"
                st.session_state.df_traditional.loc[st.session_state.df_traditional['URL'].str.contains("/twitter.com", na=False), 'Type'] = "TWITTER"
                st.session_state.df_traditional.loc[st.session_state.df_traditional['URL'].str.contains("www.instagram.com", na=False), 'Type'] = "INSTAGRAM"
                st.session_state.df_traditional.loc[st.session_state.df_traditional['URL'].str.contains("reddit.com", na=False), 'Type'] = "REDDIT"
                st.session_state.df_traditional.loc[st.session_state.df_traditional['URL'].str.contains("youtube.com", na=False), 'Type'] = "YOUTUBE"

                if merge_online:
                    st.session_state.df_traditional.Type.replace({
                        "ONLINE NEWS": "ONLINE",
                        "PRESS RELEASE": "ONLINE",
                        "BLOGS": "ONLINE"}, inplace=True)

                if "Original URL" in st.session_state.df_traditional:
                    st.session_state.df_traditional.loc[st.session_state.df_traditional["Original URL"].notnull(), "URL"] = st.session_state.df_traditional["Original URL"]

                st.session_state.df_traditional.drop(["Original URL"], axis=1, inplace=True, errors='ignore')

                # Move columns
                temp = st.session_state.df_traditional.pop('Impressions')
                st.session_state.df_traditional.insert(4, 'Impressions', temp)
                temp = st.session_state.df_traditional.pop('Mentions')
                st.session_state.df_traditional.insert(4, 'Mentions', temp)

                # Strip extra white space
                # st.session_state.df_raw['Headline'] = st.session_state.df_raw['Headline'].astype(str)
                strip_columns = ['Headline', 'Outlet', 'Author', 'Snippet']
                for column in strip_columns:

                    st.session_state.df_traditional[column] = st.session_state.df_traditional[column].str.replace('    ', ' ')
                    st.session_state.df_traditional[column] = st.session_state.df_traditional[column].str.replace('   ', ' ')
                    st.session_state.df_traditional[column] = st.session_state.df_traditional[column].str.replace('  ', ' ')


                    if st.session_state.df_traditional[column].notna().all():
                        # Strip leading and trailing spaces
                        st.session_state.df_traditional[column] = st.session_state.df_traditional[column].str.strip()

                        # Replace multiple spaces with a single space
                        st.session_state.df_traditional[column] = st.session_state.df_traditional[column].apply(
                            lambda x: " ".join(x.split()))

                        # Strip leading and trailing spaces
                        st.session_state.df_traditional[column] = st.session_state.df_traditional[column].str.strip()

                        st.session_state.df_traditional[column] = st.session_state.df_traditional[column].str.replace('& amp;', '&')





                # Remove (Online)
                st.session_state.df_traditional['Outlet'] = st.session_state.df_traditional['Outlet'].str.replace(' \\(Online\\)', '')

                # Replace wonky apostrophes
                st.session_state.df_traditional['Headline'] = st.session_state.df_traditional['Headline'].str.replace('\u2018', '\'')
                st.session_state.df_traditional['Headline'] = st.session_state.df_traditional['Headline'].str.replace('\u2019', '\'')

                # SOCIALS To sep df
                soc_array = ['FACEBOOK', 'TWITTER', 'INSTAGRAM', 'REDDIT', 'YOUTUBE']
                st.session_state.df_social = st.session_state.df_traditional.loc[st.session_state.df_traditional['Type'].isin(soc_array)]
                st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional['Type'].isin(soc_array)]

                # original_top_authors = (top_x_by_mentions(st.session_state.df_untouched, "Author"))
                original_trad_auths = (mig.top_x_by_mentions(st.session_state.df_traditional, "Author"))
                st.session_state.original_trad_auths = original_trad_auths

                # SPLIT OUT BROADCAST
                broadcast_array = ['RADIO', 'TV']
                broadcast_set = st.session_state.df_traditional.loc[st.session_state.df_traditional['Type'].isin(broadcast_array)]
                st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional['Type'].isin(broadcast_array)]

                # AP Cap
                st.session_state.df_traditional[['Headline']] = st.session_state.df_traditional[['Headline']].fillna('')
                st.session_state.df_traditional['Headline'] = st.session_state.df_traditional['Headline'].map(lambda Headline: titlecase(Headline))

                # Duplicate removal
                if drop_dupes:

                    # DROP DUPLICATES BY URL MATCHES

                    # Set aside blank URLs
                    blank_urls = st.session_state.df_traditional[st.session_state.df_traditional.URL.isna()]
                    st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional.URL.isna()]

                    # Add temporary dupe URL helper column
                    st.session_state.df_traditional['URL_Helper'] = st.session_state.df_traditional['URL'].str.lower()
                    st.session_state.df_traditional['URL_Helper'] = st.session_state.df_traditional['URL_Helper'].str.replace('http:', 'https:')

                    # Sort duplicate URLS
                    st.session_state.df_traditional = st.session_state.df_traditional.sort_values(["URL_Helper", "Author", "Impressions", "AVE", "Date"], axis=0,
                                                                                                  ascending=[True, True, False, False, True])
                    # Save duplicate URLS
                    dupe_urls = st.session_state.df_traditional[st.session_state.df_traditional['URL_Helper'].duplicated(keep='first')]

                    # Remove duplicate URLS
                    st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional['URL_Helper'].duplicated(keep='first')]

                    # Drop URL Helper column from both dfs
                    st.session_state.df_traditional.drop(["URL_Helper"], axis=1, inplace=True, errors='ignore')
                    dupe_urls.drop(["URL_Helper"], axis=1, inplace=True, errors='ignore')

                    frames = [st.session_state.df_traditional, blank_urls]
                    st.session_state.df_traditional = pd.concat(frames)

                    # DROP DUPLICATES BY COLUMN MATCHES

                    # Split off records with blank headline/outlet/type
                    blank_set = st.session_state.df_traditional[st.session_state.df_traditional.Headline.isna() | st.session_state.df_traditional.Outlet.isna() | st.session_state.df_traditional.Type.isna() | len(st.session_state.df_traditional.Headline) == 0]
                    st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional.Headline.isna()]
                    st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional.Outlet.isna()]
                    st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional.Type.isna()]

                    # Add helper column
                    st.session_state.df_traditional["dupe_helper"] = st.session_state.df_traditional['Type'].astype('string') + st.session_state.df_traditional['Outlet'].astype('string') + st.session_state.df_traditional[
                        'Headline']
                    st.session_state.df_traditional = st.session_state.df_traditional.sort_values(["dupe_helper", "Author", "Impressions", "AVE", "Date"], axis=0,
                                                                                                  ascending=[True, True, False, False, True])
                    dupe_cols = st.session_state.df_traditional[st.session_state.df_traditional['dupe_helper'].duplicated(keep='first')]
                    st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional['dupe_helper'].duplicated(keep='first')]

                    # Drop helper column and rejoin broadcast
                    st.session_state.df_traditional.drop(["dupe_helper"], axis=1, inplace=True, errors='ignore')
                    dupe_cols.drop(["dupe_helper"], axis=1, inplace=True, errors='ignore')
                    frames = [st.session_state.df_traditional, broadcast_set, st.session_state.blank_set]
                    st.session_state.df_traditional = pd.concat(frames)
                    st.session_state.df_dupes = pd.concat([dupe_urls, dupe_cols])

                else:
                    frames = [st.session_state.df_traditional, broadcast_set]
                    st.session_state.df_traditional = pd.concat(frames)

                # del st.session_state.df_traditional

                st.session_state.standard_step = True
                st.rerun()
