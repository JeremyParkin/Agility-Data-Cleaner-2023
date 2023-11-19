import mig_functions as mig
import pickle
import base64
import streamlit as st
import pandas as pd
import io
from datetime import datetime


# Set Streamlit configuration
st.set_page_config(page_title="MIG Sentiment App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png",
                   layout="wide")

# Sidebar configuration
mig.standard_sidebar()

dt_string = datetime.now().strftime("%Y-%m-%d-%H-%M")



def save_session_state():
    st.info("Processing... please wait.")
    # Manually copy necessary items from session state
    session_data = {key: value for key, value in st.session_state.items()}

    for df_name in ['df_traditional', 'df_social', 'df_dupes', 'original_trad_auths', 'auth_outlet_table', 'auth_outlet_todo',
                    'original_auths', 'df_raw', 'df_untouched', 'author_outlets', 'broadcast_set', 'blank_set',
                    'added_df', 'markdown_content', 'filtered_df', 'df_grouped', 'selected_df', 'selected_rows', 'top_stories']:
        if df_name in session_data:
            if not session_data[df_name].empty:  # Check if DataFrame is not empty
                buffer = io.StringIO()
                session_data[df_name].to_csv(buffer, index=False)
                session_data[df_name] = buffer.getvalue()
            else:
                session_data[df_name] = None  # Assign None to empty DataFrames


    # Serialize the session state
    serialized_data = pickle.dumps(session_data)

    # Encode the serialized data for downloading
    b64 = base64.b64encode(serialized_data).decode()

    # Generate a download link
    # href = f'<a href="data:file/pkl;base64,{b64}" download="session_state.pkl">Download Session State</a>'
    href = f'<a href="data:file/pkl;base64,{b64}" download="{st.session_state.client_name} - {dt_string}.pkl">Download Session File</a>'

    return href


def load_session_state(uploaded_file):
    if uploaded_file is not None:
        # Read the uploaded file
        session_data = uploaded_file.getvalue()

        # Deserialize the session state
        deserialized_data = pickle.loads(session_data)

        # List of columns to be converted to integers
        integer_columns = ['Impressions', 'Audience Reach', 'Domain Authority']  # Add your column names here

        # Convert CSV strings back to DataFrames
        for df_name in ['df_traditional', 'df_social', 'df_dupes', 'original_trad_auths', 'auth_outlet_table', 'auth_outlet_todo',
                        'original_auths', 'df_raw', 'df_untouched', 'author_outlets', 'broadcast_set', 'blank_set',
                        'added_df', 'markdown_content', 'filtered_df', 'df_grouped', 'selected_df', 'selected_rows', 'top_stories']:
            if df_name in deserialized_data:
                csv_data = deserialized_data[df_name]
                if csv_data is not None:  # Check if the CSV data is None
                    buffer = io.StringIO(csv_data)
                    deserialized_data[df_name] = pd.read_csv(buffer)

                    # Automatically convert 'Date' columns to datetime
                    if 'Date' in deserialized_data[df_name].columns:
                        deserialized_data[df_name]['Date'] = pd.to_datetime(deserialized_data[df_name]['Date'])

                    # Convert specified columns to integers
                    for col in integer_columns:
                        if col in deserialized_data[df_name].columns:
                            deserialized_data[df_name][col] = deserialized_data[df_name][col].fillna(0).astype(int)

                else:
                    # trouble shoot issues here by printing the CSV to dataframe info:
                    # st.write(f"CSV data for {df_name} is None.")
                    deserialized_data[df_name] = pd.DataFrame()  # Initialize an empty DataFrame

        # Update the session state
        st.session_state.update(deserialized_data)
        st.session_state.pickle_load = True

        st.success("Session state loaded successfully!")


st.title("Save & Load")
st.info("""**SAVE** your current data-processing session to a downloadable .pkl file
        \n**LOAD** a previously saved data-processing session from a downloaded .pkl file""")


with st.container():
    st.header("Save")
    # st.download_button("Save & Download Session", save_session_state(), type="primary")

    # When this button is clicked, the save_session_state function will be executed
    if st.button("Generate Session File to Download"):
        # Generate the download link (or any other way you handle the saving)
        href = save_session_state()

        st.markdown(href, unsafe_allow_html=True)

    st.write("")
    st.write("")
    st.write("")
    st.header("Load")
    uploaded_file = st.file_uploader("Restore a Previous Session", type="pkl", label_visibility="hidden")
    if uploaded_file is not None:
        load_session_state(uploaded_file)