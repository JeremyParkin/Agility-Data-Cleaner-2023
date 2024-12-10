import sys
import time
import streamlit as st
import pandas as pd
import mig_functions as mig
from titlecase import titlecase
import warnings
import re
warnings.filterwarnings('ignore')
st.set_page_config(layout="wide", page_title="MIG Data Processing App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")


mig.standard_sidebar()

format_dict = {'AVE': '${0:,.0f}', 'Audience Reach': '{:,d}', 'Impressions': '{:,d}'}

st.title('Standard Cleaning')


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
            with col2:
                st.subheader("Media Type")
                st.write(st.session_state.df_traditional['Type'].value_counts())
            st.subheader("Data")
            st.markdown('(Up to the first 1000 rows)')
            st.dataframe(st.session_state.df_traditional.head(1000).style.format(format_dict, na_rep=' '))

    if len(st.session_state.df_social) > 0:
        with st.expander("Social"):
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Basic Metrics")
                st.metric(label="Mentions", value="{:,}".format(len(st.session_state.df_social)))
                soc_impressions = st.session_state.df_social['Impressions'].sum()
                st.metric(label="Impressions", value=mig.format_number(soc_impressions))
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
            with col2:
                st.subheader("Media Type")
                st.write(st.session_state.df_dupes['Type'].value_counts())

            st.dataframe(st.session_state.df_dupes.style.format(format_dict, na_rep=' '))
else:
    with st.form("my_form_basic_cleaning"):
        st.subheader("Cleaning options")
        merge_online = st.checkbox("Merge 'blogs' and 'press releases' into 'Online'", value=True, help='Combine all three listed media types in the ONLINE category.')
        drop_dupes = st.checkbox("Drop duplicates", value=True, help='Remove duplicates based on identical URLS or, for non-broadcast, based on 4 identical fields: Outlet, Date, Headline, Type.')
        coverage_flags = st.checkbox("Add possible coverage flags", value=True, help='Add a column to flag stories as possible newsire coverage, possible stock moves coverage, possible market report spam, or known good outlets.')

        submitted = st.form_submit_button("Go!", type="primary")
        if submitted:
            with st.spinner("Running standard data cleaning steps..."):
                # Ensure Headline and Snippet columns are strings, even if they are blank or NaN
                for column in ["Headline", "Snippet", "Outlet"]:
                    if column in st.session_state.df_traditional.columns:
                        st.session_state.df_traditional[column] = st.session_state.df_traditional[column].fillna(
                            "").astype(str)

                start_time = time.time()

                st.session_state.df_traditional["Type"].replace({"ONLINE_NEWS": "ONLINE NEWS", "PRESS_RELEASE": "PRESS RELEASE"}, inplace=True)
                st.session_state.df_traditional.loc[st.session_state.df_traditional['URL'].str.contains("www.facebook.com", na=False), 'Type'] = "FACEBOOK"
                st.session_state.df_traditional.loc[st.session_state.df_traditional['URL'].str.contains("twitter.com", na=False), 'Type'] = "X"
                st.session_state.df_traditional.loc[
                    st.session_state.df_traditional['URL'].str.match(r'^https?://(www\.)?x\.com(/|$)',
                                                                     na=False), 'Type'] = "X"
                st.session_state.df_traditional.loc[
                    st.session_state.df_traditional['URL'].str.contains("tiktok.com", na=False), 'Type'] = "TIKTOK"
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
                soc_array = ['FACEBOOK', 'TWITTER', 'X', 'INSTAGRAM', 'REDDIT', 'YOUTUBE', 'TIKTOK']
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

                    # Add helper columns
                    st.session_state.df_traditional['Date_Helper'] = pd.to_datetime(
                        st.session_state.df_traditional['Date']).dt.strftime('%Y-%m-%d')

                    st.session_state.df_traditional["dupe_helper"] = st.session_state.df_traditional['Type'].astype('string') + st.session_state.df_traditional['Outlet'].astype('string') + st.session_state.df_traditional[
                        'Headline'] + st.session_state.df_traditional['Date_Helper'].astype('string')
                    st.session_state.df_traditional = st.session_state.df_traditional.sort_values(["dupe_helper", "Author", "Impressions", "AVE", "Date"], axis=0,
                                                                                                  ascending=[True, True, False, False, True])
                    dupe_cols = st.session_state.df_traditional[st.session_state.df_traditional['dupe_helper'].duplicated(keep='first')]
                    st.session_state.df_traditional = st.session_state.df_traditional[~st.session_state.df_traditional['dupe_helper'].duplicated(keep='first')]

                    # Drop helper column and rejoin broadcast
                    st.session_state.df_traditional.drop(["dupe_helper", "Date_Helper"], axis=1, inplace=True,
                                                         errors='ignore')
                    dupe_cols.drop(["dupe_helper", "Date_Helper"], axis=1, inplace=True, errors='ignore')
                    frames = [st.session_state.df_traditional, broadcast_set, st.session_state.blank_set]
                    st.session_state.df_traditional = pd.concat(frames)
                    st.session_state.df_dupes = pd.concat([dupe_urls, dupe_cols])



                # COVERAGE FLAGS
                # List of keywords and phrases to search for newswire junk
                newswire_phrases = [
                    "pressrelease", #
                    "accesswire", #
                    "business wire", #
                    "businesswire", #
                    "CNW", #
                    "newswire", #
                    "presswire", #
                    "openPR",
                    "pr-gateway",
                    "Prlog",
                    "PRWEB", #
                    "Pressebox",
                    "Presseportal",
                    "RTTNews",
                    "SBWIRE",
                    "issuewire",
                    "prunderground"
                ]

                stock_moves_phrases = [
                    "ADVFN", "ARIVA.DE", "Benzinga", "Barchart", "Daily Advent", "ETF Daily News",
                    "FinanzNachrichten.de", "Finanzen.at", "Finanzen.ch", "FONDS exclusiv", "Market Beat",
                    "Market Newsdesk", "Market Newswire", "Market Screener", "Market Wire News", "MarketBeat",
                    "MarketScreener", "MarketWatch", "Nasdaq", "Seeking Alpha", "Stock Observer", "Stock Titan",
                    "Stockhouse", "Stockstar", "Zacks"

                ]

                # Define a list of aggregator outlet names
                aggregators_list = [
                    "Yahoo", "MSN", "News Break", "Google News", "Apple News", "Flipboard",
                    "Pocket", "Feedly", "SmartNews", "StumbleUpon"
                ]

                # Define a list of reputable outlet names
                outlet_names = [
                    "National Post",
                    "The Canadian Press",
                    "The Globe and Mail",
                    "Toronto Star",
                    "Calgary Herald",
                    "Edmonton Journal",
                    "Montreal Gazette",
                    "Ottawa Citizen",
                    "The Chronicle Herald",
                    "The Telegram",
                    "Vancouver Sun",
                    "Winnipeg Free Press",
                    "The Globe",
                    "Toronto Life",
                    "BlogTO",
                    "CBC News",
                    "CBC ",
                    "CityNews",
                    "City ",
                    "Citytv ",
                    "CTV ",
                    "CP24",
                    "Daily Hive",
                    "Global News",
                    "La Presse",
                    "Le Devoir",
                    "Le Journal de Montréal",
                    "Radio-Canada",
                    "BNN Bloomberg",
                    "Financial Post",
                    "rabble.ca",
                    "The Tyee",
                    "The Walrus",
                    "CHCH",
                    "CHEK News",
                    "NOW Magazine",
                    "The Georgia Straight",
                    "HuffPost Canada",
                    "iPolitics",
                    "TVO.org",
                    "OMNI Television",
                    "Sing Tao Daily",
                    "APTN National News",
                    "Calgary Sun",
                    "Edmonton Sun",
                    "Hamilton Spectator",
                    "Kingston Whig-Standard",
                    "London Free Press",
                    "Ottawa Sun",
                    "Regina Leader-Post",
                    "Sault Star",
                    "StarPhoenix",
                    "Sudbury Star",
                    "The Province",
                    "Toronto Sun",
                    "Windsor Star",
                    "Winnipeg Sun",
                    "Bloomberg",
                    "Financial Times",
                    "Macleans",
                    "Reuters",
                    "Journal de Quebec",
                    "L'Actualite",
                    "Le Droit",
                    "Le Soleil",
                    "Les Affaires",
                    "TVA Nouvelles",
                    "Times Colonist",
                    "The New York Times",
                    "The Washington Post",
                    "USA Today",
                    "Los Angeles Times",
                    "Chicago Tribune",
                    "The Boston Globe",
                    "The Dallas Morning News",
                    "The Philadelphia Inquirer",
                    "San Francisco Chronicle",
                    "Miami Herald",
                    "The Seattle Times",
                    "Houston Chronicle",
                    "The Salt Lake Tribune",
                    "Deseret News",
                    "Albany Times Union",
                    "Arkansas Democrat-Gazette",
                    "Austin American-Statesman",
                    "Bakersfield Californian",
                    "Buffalo News",
                    "Charleston Gazette-Mail",
                    "The Columbus Dispatch",
                    "The Fresno Bee",
                    "Hartford Courant",
                    "Idaho Statesman",
                    "Las Vegas Review-Journal",
                    "The Ledger",
                    "Lexington Herald-Leader",
                    "The Modesto Bee",
                    "The Morning Call",
                    "New Haven Register",
                    "Omaha World-Herald",
                    "Palm Beach Post",
                    "Patriot-News",
                    "Pittsburgh Post-Gazette",
                    "Richmond Times-Dispatch",
                    "The Sacramento Bee",
                    "The Spokesman-Review",
                    "Syracuse Post-Standard",
                    "The Tennessean",
                    "The Trentonian",
                    "Tulsa World",
                    "The Virginian-Pilot",
                    "The Wichita Eagle",
                    "The Star-Ledger",
                    "The News & Observer",
                    "The News Tribune",
                    "Reno Gazette-Journal",
                    "The Clarion-Ledger",
                    "The State",
                    "Daily Press",
                    "The Ann Arbor News",
                    "The Day",
                    "The Press-Enterprise",
                    "South Florida Sun Sentinel",
                    "The Providence Journal",
                    "Daily Herald",
                    "The Times-Picayune/The New Orleans Advocate",
                    "The Star Press",
                    "The Pueblo Chieftain",
                    "The Record",
                    "The Roanoke Times",
                    "The Daily Breeze",
                    "The Vindicator",
                    "Waco Tribune-Herald",
                    "Yakima Herald-Republic",
                    "York Daily Record",
                    "NPR",
                    "ABC News",
                    "NBC News",
                    "CBS News",
                    "CNN",
                    "Fox News",
                    "CNBC",
                    "The Wall Street Journal",
                    "Barron's",
                    "ProPublica",
                    "The Atlantic",
                    "Politico",
                    "Vox",
                    "Slate",
                    "The Nation",
                    "Mother Jones",
                    "The Hill",
                    "Axios",
                    "BuzzFeed News",
                    "Vice News",
                    "HuffPost",
                    "The Verge",
                    "Univision",
                    "Telemundo",
                    "Indian Country Today",
                    "The Detroit News",
                    "New York Post",
                    "San Diego Union-Tribune",
                    "The Baltimore Sun",
                    "Orlando Sentinel",
                    "The Denver Post",
                    "The Plain Dealer",
                    "The Charlotte Observer",
                    "St. Louis Post-Dispatch",
                    "The Kansas City Star",
                    "The Tampa Bay Times",
                    "The Star Tribune",
                    "Milwaukee Journal Sentinel",
                    "The Indianapolis Star",
                    "The Courier-Journal",
                    "The Times",
                    "The Guardian",
                    "The Daily Telegraph",
                    "The Independent",
                    "The Sun",
                    "The Daily Mail",
                    "The Mirror",
                    "The Observer",
                    "The Sunday Times",
                    "The Evening Standard",
                    "Yorkshire Post",
                    "The Scotsman",
                    "Manchester Evening News",
                    "Liverpool Echo",
                    "Birmingham Mail",
                    "Wales Online",
                    "Belfast Telegraph",
                    "The Herald Scotland",
                    "ITV News",
                    "BBC News",
                    "Channel 4 News",
                    "Sky News",
                    "Reuters UK",
                    "City A.M.",
                    "The Economist",
                    "The Spectator",
                    "New Statesman",
                    "The Week",
                    "Prospect Magazine",
                    "The Conversation UK",
                    "HuffPost UK",
                    "Metro",
                    "The Register",
                    "PinkNews",
                    "Al Jazeera English (UK)",
                    "The National (Scotland)",
                    "The Courier (Dundee)",
                    "Cambridge News",
                    "Eastern Daily Press",
                    "Oxford Mail",
                    "Swindon Advertiser",
                    "The Argus (Brighton)",
                    "Kent Online",
                    "Lincolnshire Echo",
                    "Gloucestershire Live"
                ]

                st.write(f"Standard Cleaning completed in {time.time() - start_time:.2f} seconds.")

                if coverage_flags:
                    # Create temporary flagging columns
                    st.session_state.df_traditional["Newswire Flag"] = ""
                    st.session_state.df_traditional["Market Report Flag"] = ""
                    st.session_state.df_traditional["Stock Moves Flag"] = ""
                    st.session_state.df_traditional["Good Outlet Flag"] = ""
                    st.session_state.df_traditional["Aggregator Flag"] = ""
                    st.session_state.df_traditional["Coverage Flags"] = ""

                    # NEWSWIRE MASK - Precompute boolean masks for each condition
                    start_time = time.time()


                    # Function to extract first and last 125 words of a snippet
                    def extract_relevant_text(snippet):
                        words = snippet.split()
                        if len(words) > 250:
                            return " ".join(words[:125] + words[-125:])
                        return snippet


                    # Apply the function to preprocess the Snippet column
                    st.session_state.df_traditional["Snippet_Limited"] = st.session_state.df_traditional[
                        "Snippet"].apply(extract_relevant_text)

                    # NEWSWIRE MASK - Create the newswire mask on the limited snippet text (first and last 125 words)
                    newswire_mask = st.session_state.df_traditional["Snippet_Limited"].str.contains(
                        "|".join(re.escape(phrase) for phrase in newswire_phrases),
                        case=False,
                        na=False,
                        regex=True
                    )

                    # Optional: Drop or keep the "Snippet_Limited" column based on your needs
                    st.session_state.df_traditional.drop(columns=["Snippet_Limited"], inplace=True)
                    st.write(f"Newswires flagged in {time.time() - start_time:.2f} seconds.")


                    # STOCK MOVES MASK - based on outlet names
                    start_time = time.time()
                    stock_moves_mask = st.session_state.df_traditional["Outlet"].str.contains(
                        "|".join(re.escape(phrase) for phrase in stock_moves_phrases),
                        case=False,
                        na=False,
                        regex=True
                    )
                    st.write(f"Stock & Financials flagged in {time.time() - start_time:.2f} seconds.")


                    # MARKET SPAM MASK - Create a mask for rows where both "global" and "market" are present in the Headline column
                    start_time = time.time()
                    market_report_mask = (
                            st.session_state.df_traditional["Headline"]
                            .str.contains(r"\bglobal\b", case=False, na=False, regex=True) &
                            st.session_state.df_traditional["Headline"]
                            .str.contains(r"\bmarket\b", case=False, na=False, regex=True)
                    )
                    st.write(f"Market Report Spam flagged in {time.time() - start_time:.2f} seconds.")


                    # GOOD OUTLETS MASK - based on outlet name
                    start_time = time.time()
                    # Create a mask for reputable outlets
                    reputable_outlet_mask = st.session_state.df_traditional["Outlet"].str.contains(
                        "|".join(map(re.escape, outlet_names)), case=False, na=False
                    )
                    st.write(f"Good outlets flagged in {time.time() - start_time:.2f} seconds.")



                    # AGGREGATORS MASK
                    start_time = time.time()
                    # Create a mask for outlets matching any aggregator name
                    aggregator_mask = st.session_state.df_traditional["Outlet"].str.contains(
                        "|".join(re.escape(name) for name in aggregators_list),
                        case=False,
                        na=False,
                        regex=True
                    )
                    st.write(f"Aggregators flagged in {time.time() - start_time:.2f} seconds.")


                    # Assign flags based on priority
                    # start_time = time.time()
                    st.session_state.df_traditional.loc[newswire_mask, "Newswire Flag"] = "Newswire?"
                    # st.write(f"Newswire flagging completed in {time.time() - start_time:.2f} seconds.")

                    # start_time = time.time()
                    st.session_state.df_traditional.loc[
                        ~newswire_mask & market_report_mask, "Market Report Flag"] = "Market Report Spam?"
                    # st.write(f"Market report flagging completed in {time.time() - start_time:.2f} seconds.")

                    # start_time = time.time()
                    st.session_state.df_traditional.loc[
                        ~newswire_mask & ~market_report_mask & stock_moves_mask, "Stock Moves Flag"] = "Stocks / Financials?"
                    # st.write(f"Stock Moves flagging completed in {time.time() - start_time:.2f} seconds.")

                    # start_time = time.time()
                    # Apply the flag for reputable outlets
                    st.session_state.df_traditional.loc[
                        ~newswire_mask & ~market_report_mask & ~stock_moves_mask & reputable_outlet_mask, "Good Outlet Flag"] = "Good Outlet"
                    # st.write(f"Reputable Outlet flagging completed in {time.time() - start_time:.2f} seconds.")

                    # start_time = time.time()
                    st.session_state.df_traditional.loc[aggregator_mask, "Aggregator Flag"] = "Aggregator"
                    # st.write(f"News aggregator flagging completed in {time.time() - start_time:.2f} seconds.")


                    def combine_flags(row):
                        # Use .get() to avoid KeyError if a column is missing
                        if row.get("Newswire Flag"):
                            return row["Newswire Flag"]
                        elif row.get("Good Outlet Flag"):
                            return row["Good Outlet Flag"]
                        elif row.get("Aggregator Flag"):
                            return row["Aggregator Flag"]
                        elif row.get("Market Report Flag"):
                            return row["Market Report Flag"]
                        elif row.get("Stock Moves Flag"):
                            return row["Stock Moves Flag"]
                        return ""


                    st.session_state.df_traditional["Coverage Flags"] = st.session_state.df_traditional.apply(
                        combine_flags, axis=1)

                    # Drop individual flag columns
                    flag_columns = ["Newswire Flag", "Good Outlet Flag", "Market Report Flag", "Stock Moves Flag", "Aggregator Flag"]
                    st.session_state.df_traditional.drop(
                        columns=[col for col in flag_columns if col in st.session_state.df_traditional], inplace=True)



                else:
                    frames = [st.session_state.df_traditional, broadcast_set]
                    st.session_state.df_traditional = pd.concat(frames)


                st.session_state.standard_step = True
                st.rerun()