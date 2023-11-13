import pandas as pd
import streamlit as st
import warnings
import mig_functions as mig

warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(layout="wide", page_title="MIG Data Cleaning App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")


# Define global variables for the DataFrame names
df_vars = ['filtered_df', 'df_grouped', 'selected_df', 'selected_rows', 'top_stories']

# Initialize session state variables
for var in df_vars:
    if var not in st.session_state:
        st.session_state[var] = pd.DataFrame()

if 'added_df' not in st.session_state:
    st.session_state['added_df'] = pd.DataFrame(columns=['Headline', 'Mentions', 'Impressions','Example Snippet', 'Example URL', 'Example Date'])

# Sidebar configuration
mig.standard_sidebar()

# Page title
st.title('Top Stories')

if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')
elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')
else:
    st.session_state['filtered_df'] = st.session_state.df_traditional.copy()

    # Example of filling NaN values
    st.session_state.filtered_df["Impressions"].fillna(0, inplace=True)
    st.session_state.filtered_df["Snippet"].fillna("", inplace=True)


    trend_df = st.session_state['filtered_df']
    trend_df['Date'] = pd.to_datetime(trend_df['Date']).dt.date
    summary_stats = trend_df.groupby("Date").agg({"Mentions": "count", "Impressions": "sum"}).sort_index()
    summary_stats.index.name = "Date"
    summary_stats.reset_index(inplace=True)

    # Add one day to 'Date' to compensate for the offset in the chart
    summary_stats['Date'] = summary_stats['Date'] + pd.Timedelta(days=1)



    col1, col2 = st.columns([2,3], gap="medium")

    with col1:
        tab1, tab2 = st.tabs(['Mention Trend', 'Impressions Trend'])
        with tab1:
            st.area_chart(data=summary_stats, x="Date", y="Mentions", width=0, height=200, use_container_width=True) #1 day early here
    with tab2:
        st.area_chart(data=summary_stats, x="Date", y="Impressions", width=0, height=200, use_container_width=True) #1 day early here


    # Group by 'Headline' and aggregate the 'Mentions' and 'Impressions'
    st.session_state['df_grouped'] = st.session_state.filtered_df.groupby('Headline').agg(
        {'Mentions': 'count', 'Impressions': 'sum'}).reset_index()



    # Define functions to pick the best snippet and URL
    def pick_best_snippet(group):
        if not group.empty:
            best_snippet = group.loc[group['Snippet'].str.len().idxmax()]['Snippet']
            if pd.isna(best_snippet) or best_snippet.strip() == '':
                best_snippet = group.loc[group['Impressions'].idxmax()]['Snippet']
            return best_snippet
        return None  # Return None or some default value if the group is empty


    def pick_best_story_details(group):
        if not group.empty and 'Impressions' in group and not group['Impressions'].isnull().all():
            # Define the middle and bottom tiers
            middle_tier_keywords = ['MarketWatch', 'Seeking Alpha', "News Break"]
            bottom_tier_keywords = ['Yahoo', 'MSN']

            # Exclude or deprioritize middle and bottom tier outlets
            top_tier_group = group[
                ~group['Outlet'].str.contains('|'.join(middle_tier_keywords + bottom_tier_keywords), case=False,
                                              na=False)]
            middle_tier_group = group[
                group['Outlet'].str.contains('|'.join(middle_tier_keywords), case=False, na=False) &
                ~group['Outlet'].str.contains('|'.join(bottom_tier_keywords), case=False, na=False)]

            # Select from top tier if available
            if not top_tier_group.empty:
                best_row = top_tier_group.loc[top_tier_group['Impressions'].idxmax()]
                return best_row['Outlet'], best_row['Date'], best_row.get('URL',
                                                                          None)  # Handle missing 'Example URL'

            # If top tier is empty, select from middle tier
            if not middle_tier_group.empty:
                best_row = middle_tier_group.loc[middle_tier_group['Impressions'].idxmax()]
                return best_row['Outlet'], best_row['Date'], best_row.get('URL', None)

            # Otherwise, fall back to the original group (bottom tier)
            best_row = group.loc[group['Impressions'].idxmax()]
            return best_row['Outlet'], best_row['Date'], best_row.get('URL', None)

        return None, None, None  # Return None for each if the group is empty or if 'Impressions' is all NaN


    def pick_best_type(group):
        if not group.empty and 'Impressions' in group and not group['Impressions'].isnull().all():
            # Exclude or deprioritize rows where the 'Outlet' contains 'Yahoo' or 'MSN'
            non_aggregator_group = group[~group['Outlet'].str.contains('Yahoo|MSN', case=False, na=False)]
            # If non-aggregator group is not empty, pick the best URL from it
            if not non_aggregator_group.empty:
                return non_aggregator_group.loc[non_aggregator_group['Impressions'].idxmax()]['Type']
            # Otherwise, fall back to the original group
            return group.loc[group['Impressions'].idxmax()]['Type']
        return None  # Return None or some default value if the group is empty or if 'Impressions' is all NaN



    # Define a function to save selected rows without the 'Top Story' column
    def save_selected_rows(updated_data, key):
        selected_rows = updated_data.loc[updated_data['Top Story'] == True].copy()
        selected_rows.drop(columns=['Top Story'], inplace=True)  # Drop the checkbox column
        st.session_state.selected_rows = selected_rows

        if st.button("Save Selected to Top Stories Table", key=key, type="primary"):
            # Concatenate while keeping all columns except 'Top Story'
            st.session_state.added_df = pd.concat([st.session_state.added_df, selected_rows])
            st.session_state.added_df.drop_duplicates(subset=["Headline"], keep='last', inplace=True)



    @st.cache_data
    def group_and_process_data(df):
        # Perform grouping and processing
        grouped_df = df.groupby('Headline').agg({'Mentions': 'count', 'Impressions': 'sum'}).reset_index()

        # Update this section to use the new function
        for i, row in grouped_df.iterrows():
            headline_group = df[df['Headline'] == row['Headline']]
            best_outlet, best_date, best_url = pick_best_story_details(headline_group)
            grouped_df.at[i, 'Example Outlet'] = best_outlet
            grouped_df.at[i, 'Example URL'] = best_url
            grouped_df.at[i, 'Example Date'] = best_date

        # Continue with the rest of the processing
        grouped_df['Example Snippet'] = grouped_df['Headline'].apply(
            lambda x: pick_best_snippet(df[df['Headline'] == x]))
        grouped_df['Example Type'] = grouped_df['Headline'].apply(lambda x: pick_best_type(df[df['Headline'] == x]))

        # Convert datetime to date-only format
        if 'Example Date' in grouped_df.columns and pd.notnull(grouped_df['Example Date']).any():
            grouped_df['Example Date'] = pd.to_datetime(grouped_df['Example Date']).dt.date

        return grouped_df


    # Then use this function to get the processed data
    st.session_state['df_grouped'] = group_and_process_data(st.session_state.filtered_df)

    # Sort the DataFrame first by 'Mentions' in descending order, then by 'Impressions' also in descending order
    st.session_state['df_grouped'] = st.session_state['df_grouped'].sort_values(by=['Mentions', 'Impressions'],
                                                                                ascending=[False, False])

    with col2:
        st.write("Custom Filter")

        # Check if 'custom_filter' exists in session state, if not initialize it.
        if 'custom_filter' not in st.session_state:
            st.session_state.custom_filter = None

        # Define the streamlit form for user input
        with st.form(key='my_form'):
            # Create a dropdown list to select a column
            tag_columns = [col for col in st.session_state.df_traditional.columns if col.startswith('Tag')]
            tag_columns.insert(0, 'Headline')

            col1, col2  = st.columns(2, gap="medium")
            with col1:
                selected_column = st.selectbox('Select a column:', tag_columns)
            with col2:
                # Create an input box to enter the keyword
                column_keyword = st.text_input('Contains')

            submit_button = st.form_submit_button(label='Apply Filter')

        # Button to clear the custom filter
        if st.button('Clear Filter'):
            st.session_state.custom_filter = None
            st.experimental_rerun()

        if submit_button:
            # Filter the dataframe based on user inputs
            if column_keyword:
                filtered_df = st.session_state.filtered_df[
                    st.session_state.filtered_df[selected_column].fillna('').str.contains(column_keyword, case=False)]
                st.session_state.custom_filter = group_and_process_data(filtered_df)

    # Use the custom filter if it exists, else use the grouped dataframe
    df_to_display = st.session_state.custom_filter if st.session_state.custom_filter is not None else st.session_state.df_grouped

    df_grouped_custom = df_to_display.copy()
    df_grouped_custom['Example URL'] = df_grouped_custom['Example URL'].astype(str)

    # Sort the DataFrame first by 'Mentions' in descending order, then by 'Impressions' also in descending order
    df_grouped_custom = df_grouped_custom.sort_values(by=['Mentions', 'Impressions'],
                                                                                ascending=[False, False])

    df_grouped_custom["Top Story"] = False

    st.subheader("Possible Top Stories")
    updated_data_custom = st.data_editor(df_grouped_custom,
                                         key="df_by_custom",
                                         use_container_width=True,
                                         column_config={
                                             "Headline": st.column_config.Column("Headline", width="large"),
                                             "Example URL": st.column_config.LinkColumn("Example URL", width="medium"),
                                             "Example Snippet": st.column_config.Column("Example Snippet", width="medium"),
                                             "Example Outlet": None,
                                             "Example Type": None,
                                         },
                                         hide_index=True,
                                         )

    # Use the function to save the selected rows
    save_selected_rows(updated_data_custom, key="by_custom")


    # Display the saved top stories with the best snippet and URL
    if len(st.session_state['added_df']) > 0:
        st.subheader("Saved Top Stories")
        st.dataframe(st.session_state['added_df'],
                                            column_config={
                                             "Headline": st.column_config.Column("Headline", width="large"),
                                             "Example URL": st.column_config.LinkColumn("Example URL", width="medium"),
                                             "Example Snippet": st.column_config.Column("Example Snippet", width="medium"),
                                             "Example Outlet": None,
                                             "Example Type": None,
                                            },
                                         hide_index=True,
                     )

        # Clear saved top stories
        if st.button("Clear Saved"):
            st.session_state['added_df'] = pd.DataFrame(columns=['Headline', 'Mentions', 'Impressions','Example Snippet', 'Example URL', 'Example Date'])
            st.experimental_rerun()