import mig_functions as mig
import dill
import base64
import streamlit as st
import pandas as pd
import io
from datetime import datetime


# Set Streamlit configuration
st.set_page_config(layout="wide", page_title="MIG Data Processing App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2025/01/favicon.png")

# Sidebar configuration
mig.standard_sidebar()

dt_string = datetime.now().strftime("%Y-%m-%d-%H-%M")


df_names = st.session_state.df_names  # List of DataFrames in the session state


# def save_session_state():
#     # Manually copy necessary items from session state
#     session_data = {key: value for key, value in st.session_state.items() if key not in st.session_state.df_names}
#
#     # Directly serialize DataFrames in session state
#     for df_name in st.session_state.df_names:
#         if df_name in st.session_state and not st.session_state[df_name].empty:
#             session_data[df_name] = st.session_state[df_name]  # Save DataFrame directly
#
#     # Serialize the session state
#     serialized_data = dill.dumps(session_data)
#
#     # Provide downloadable file link
#     file_name = f"{st.session_state.client_name} - {dt_string}.pkl"
#     st.download_button(label="Download Session File",
#                        data=serialized_data,
#                        file_name=file_name,
#                        mime="application/octet-stream")


def save_session_state():
    session_data = {k: v for k, v in st.session_state.items()
                    if k not in st.session_state.df_names}

    # Save df_names explicitly so LOAD can use it
    session_data["df_names"] = st.session_state.df_names

    # Save every listed DF (even if empty, if you prefer)
    for df_name in st.session_state.df_names:
        if df_name in st.session_state:
            session_data[df_name] = st.session_state[df_name]

    serialized_data = dill.dumps(session_data)
    file_name = f"{st.session_state.client_name} - {dt_string}.pkl"
    st.download_button(
        label="Download Session File",
        data=serialized_data,
        file_name=file_name,
        mime="application/octet-stream",
    )



# def load_session_state(uploaded_file):
#     if uploaded_file is not None:
#         # Read the uploaded file
#         session_data = dill.loads(uploaded_file.read())
#
#         # Check for and restore DataFrames
#         for df_name in st.session_state.df_names:
#             if df_name in session_data:
#                 data = session_data[df_name]
#                 # Check if the data is a CSV string (legacy format)
#                 if isinstance(data, str) and "\n" in data:  # Simple heuristic to identify CSV content
#                     buffer = io.StringIO(data)
#                     df = pd.read_csv(buffer)
#
#                     # Automatically convert 'Date' columns to datetime
#                     if 'Date' in df.columns:
#                         df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
#
#                     # Restore DataFrame to session state
#                     st.session_state[df_name] = df
#                 else:
#                     # Assume it's already a DataFrame in the new format
#                     st.session_state[df_name] = data
#
#         # Update non-DataFrame variables in session state
#         for key, value in session_data.items():
#             if key not in st.session_state.df_names:
#                 st.session_state[key] = value
#
#         st.session_state.pickle_load = True
#         st.success("Session state loaded successfully!")

def load_session_state(uploaded_file):
    if uploaded_file is not None:
        uploaded_file.seek(0)
        session_data = dill.loads(uploaded_file.read())

        # Restore non-DF scalars first (including df_names if present)
        for key, value in session_data.items():
            if not isinstance(value, (pd.DataFrame, str)):
                st.session_state[key] = value

        # Restore any DataFrame or legacy CSV-string, regardless of df_names
        restored_df_names = []
        for key, value in session_data.items():
            if isinstance(value, pd.DataFrame):
                st.session_state[key] = value
                restored_df_names.append(key)
            elif isinstance(value, str) and "\n" in value:
                df = pd.read_csv(io.StringIO(value))
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                st.session_state[key] = df
                restored_df_names.append(key)

        # Ensure df_names reflects what we actually restored
        st.session_state.df_names = restored_df_names

        st.session_state.pickle_load = True
        st.success("Session state loaded successfully!")



st.title("Save & Load")
st.divider()

st.header("Save")

if not st.session_state.upload_step:
    st.error('Please upload a CSV before SAVING.')

elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')

else:
    st.info("**SAVE** your current data-processing session to a downloadable .pkl file")



    # When this button is clicked, the save_session_state function will be executed
    if st.button("Generate Session File to Download"):
        placeholder = st.empty()
        placeholder.info("Processing... please wait.")
        # Generate the download link
        href = save_session_state()

        # Show the download link
        if href:  # Only display if href is not None
            st.markdown(href, unsafe_allow_html=True)
        placeholder.empty()



    st.divider()

st.header("LOAD")
st.info("**LOAD** a previously saved data-processing session from a downloaded .pkl file")

uploaded_file = st.file_uploader("Restore a Previous Session", type="pkl", label_visibility="hidden")
if uploaded_file is not None:
    load_session_state(uploaded_file)