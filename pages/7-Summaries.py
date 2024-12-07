import pandas as pd
import streamlit as st
import warnings
import mig_functions as mig
import openai
import time
import numpy as np
from datetime import datetime
import re
from openai import OpenAI


type_dict = {
    "RADIO": "broadcast transcript",
    "TV": "broadcast transcript",
    "PODCAST": "broadcast transcript",
    "ONLINE": "online article",
    "PRINT": "print article",
}

warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(layout="wide", page_title="MIG Data Processing App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")

# Standard sidebar
mig.standard_sidebar()

# Page title
st.title("Top Stories Summaries")

if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')
elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')
elif len(st.session_state['added_df']) == 0:
    st.error('Please select your TOP STORIES before trying this step.')

else:
    client = OpenAI(api_key=st.secrets["key"])

    # Load the DataFrame with top stories
    df = st.session_state.added_df

    # Sort the DataFrame by Date from oldest to newest
    df = df.sort_values(by='Date', ascending=True).reset_index(drop=True)

    # Form for user input
    # User input for analysis mode and named entity
    col1, col2 = st.columns(2, gap="medium")
    with col1:
        mode = st.radio("Analysis Mode", ('Summary', 'Sentiment'),
                        help='Produce a short summary or sentiment opinion relative to the client brand for the selected top stories.',
                        key='analysis_mode')
    with col2:
        if mode == 'Summary':
            summary_length = st.radio("Summary Length", ('Short (20-25 words)', 'Long (50-60 words)'),
                                      help='Select the length of the summary for the selected top stories.',
                                      key='summary_length')
        else:
            summary_length = 'Short (20-25 words)'

    named_entity = st.text_input("Enter the named entity:", max_chars=100, key="named_entity",
                                 help="The brand, organization, or person this analysis should focus on",
                                 value=st.session_state.client_name)

    # Dynamically determine the column name
    summary_column_name = f"Short Entity Summary" if summary_length == "Short (20-25 words)" else f"Long Entity Summary"

    submitted = st.button("Submit")


    # Check if the form is submitted and named entity is not empty
    if submitted and named_entity:
        # Set OpenAI API key
        openai.api_key = st.secrets["key"]


        # Define function to generate summary prompt
        def generate_summary_prompt(row, named_entity, length):
            length_text = "20-25 words" if length == 'Short (20-25 words)' else "50-60 words"

            if row['Example Type'] == "RADIO" or row['Example Type'] == "TV":
                summary_prompt = f"""
                    Provide an executive content analysis of {named_entity} in the following broadcast transcript. Note that
                    broadcast transcripts often contain clips of unrelated advertisements and other segments that should be ignored.
                    The summary should be concise, approximately {length_text}, and should not include any 
                    labels or introductory text (not even the word 'Summary'). 
                    \n\nHEADLINE:\n{row['Headline']}\n BODY: \n{row['Example Snippet']}"
                    """

            else:
                summary_prompt = f"""
                       Provide an executive content analysis of {named_entity} in the following {type_dict[row['Example Type']]}. 
                       The summary should be concise, approximately {length_text}, and should not include any 
                       labels or introductory text (not even the word 'Summary'). 
                       \n\nHEADLINE:\n{row['Headline']}\n BODY: \n{row['Example Snippet']}"
                       """

            return summary_prompt


        # Define function to generate sentiment prompt
        def generate_sentiment_prompt(row, named_entity):
            return f"Please indicate the sentiment of the following news story as it relates to {named_entity}. Start with one word: Positive, Neutral, or Negative - followed by a colon then a one sentence rationale as to why that sentiment was chosen.\n\nThis is the news story:\n{row['Headline']}. {row['Example Snippet']}"


        # Initialize progress bar
        progress_bar = st.progress(0)
        total_items = len(df)  # Get the total number of items to process
        processed_items = 0  # Initialize the number of processed items

        # Loop through each row in the DataFrame to generate summaries or sentiments
        for i, row in df.iterrows():
            try:
                # Check if the snippet is long enough to generate a response
                if len(row['Example Snippet']) < 150:
                    st.warning(f"Snippet is too short for message {i + 1}.")
                    if mode == "Summary":
                        df.at[i, summary_column_name] = "Snippet too short to generate summary"
                    else:  # Sentiment mode
                        df.at[i, 'Entity Sentiment'] = "Snippet too short to generate sentiment opinion"

                    continue

                # Generate prompt based on mode
                if mode == "Summary":
                    prompt = generate_summary_prompt(row, named_entity, summary_length)
                    df.at[i, summary_column_name] = ""
                else:  # Sentiment mode
                    prompt = generate_sentiment_prompt(row, named_entity)
                    df.at[i, 'Entity Sentiment'] = ""

                # Call the OpenAI API using the chat interface
                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are a highly knowledgeable media analysis AI."},
                        {"role": "user", "content": prompt}
                    ]
                )

                # Update progress bar after each item is processed
                processed_items += 1
                progress = int((processed_items / total_items) * 100)
                progress_bar.progress(progress)

                # Update the DataFrame with the response
                if mode == "Summary":
                    summary = response.choices[0].message.content.strip()
                    df.at[i, summary_column_name] = summary
                else:
                    sentiment = response.choices[0].message.content.strip()
                    df.at[i, 'Entity Sentiment'] = sentiment




            except openai.RateLimitError:
                st.warning("Rate limit exceeded. Waiting for 20 seconds before retrying.")
                time.sleep(20)

            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")

        # Complete the progress bar when done
        progress_bar.progress(100)

        # **SAVE UPDATED DATAFRAME BACK TO SESSION STATE**
        st.session_state.added_df = df



    else:
        if submitted and not named_entity:
            st.error('Named entity is required to proceed.')


    markdown_content = ""


    def escape_markdown(text):
        # List of Markdown special characters to escape
        # markdown_special_chars = r"\`*_{}[]()#+-.!$"
        # escaped_text = re.sub(r"([{}])".format(re.escape(markdown_special_chars)), r"\\\1", text)
        # return escaped_text
        # List of Markdown special characters to escape
        markdown_special_chars = r"\`*_{}[]()#+-.!$"
        # Correctly form the regular expression pattern
        pattern = r"([" + re.escape(markdown_special_chars) + r"])"
        escaped_text = re.sub(pattern, r"\\\1", text)
        return escaped_text


    # # Checkboxes for displaying additional information
    col3, col4 = st.columns(2, gap="medium")
    with col3:
        show_mentions = st.checkbox("Show mentions", value=False)
        show_impressions = st.checkbox("Show impressions", value=False)
        if "Entity Sentiment" in df.columns:
            show_sentiment = st.checkbox("Show sentiment", value=True)
    with col4:
        if "Short Entity Summary" in df.columns:
            show_short_summary = st.checkbox("Show short summary", value=True)
        if "Long Entity Summary" in df.columns:
            show_long_summary = st.checkbox("Show long summary", value=False)

    # Show warning if show_mentions or show_impressions is checked
    if show_mentions or show_impressions:
        st.warning(
            "WARNING: Mentions and Impressions totals reflect exact match headlines on the same date only. Totals may not include coverage with variations in headline wording or publication dates.")

    st.write(" ")

    for story in df.index:


        head = escape_markdown(df["Headline"][story])
        outlet = escape_markdown(df["Example Outlet"][story])
        link = escape_markdown(df["Example URL"][story])
        date = df["Date"][story].strftime("%B %d, %Y")
        # df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
        # date = df.at[story, "Date"].strftime("%B %d, %Y")



        # Build the markdown string
        markdown_content += f"__[{head}]({link})__  \n"
        markdown_content += f"_{outlet}_ – {date}  \n"

        if "Short Entity Summary" in df.columns:
            if show_short_summary:
                entity_summary = df["Short Entity Summary"][story]
                markdown_content += f"{entity_summary}  \n\n"

        if "Long Entity Summary" in df.columns:
            if show_long_summary:
                entity_summary = df["Long Entity Summary"][story]
                markdown_content += f"{entity_summary}  \n\n"

        if "Entity Sentiment" in df.columns:
            if show_sentiment:
                entity_sentiment = df["Entity Sentiment"][story]
                # markdown_content += "<br>"
                markdown_content += f"_{entity_sentiment}_  \n\n"

        if show_mentions:
            mentions = df['Mentions'][story]
            markdown_content += f"**Mentions**: {mentions} &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"

        if show_impressions:
            impressions = df['Impressions'][story]
            markdown_content += f"**Impressions**: {impressions:,}"

        if show_mentions or show_impressions:
            markdown_content += "<br>"

        markdown_content += "<br>"

    # Display the entire content as Markdown
    st.markdown(markdown_content, unsafe_allow_html=True)