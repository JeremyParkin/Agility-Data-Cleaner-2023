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




def load_session_state(uploaded_file):
    if uploaded_file is not None:
        uploaded_file.seek(0)
        session_data = dill.loads(uploaded_file.read())

        # Determine dataframe keys from saved session (fallback to current list)
        saved_df_names = session_data.get("df_names", st.session_state.get("df_names", []))

        # Restore DataFrames (including legacy CSV strings) only for known dataframe keys
        restored_df_names = []
        for df_name in saved_df_names:
            if df_name not in session_data:
                continue

            value = session_data[df_name]
            if isinstance(value, pd.DataFrame):
                st.session_state[df_name] = value
                restored_df_names.append(df_name)
            elif isinstance(value, str):
                try:
                    df = pd.read_csv(io.StringIO(value))
                except Exception:
                    # Not a legacy CSV dataframe payload
                    continue
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                st.session_state[df_name] = df
                restored_df_names.append(df_name)

        # Restore non-dataframe session variables (including string values like client_name)
        for key, value in session_data.items():
            if key in restored_df_names or key == "df_names":
                continue
            st.session_state[key] = value

        st.session_state.df_names = restored_df_names if restored_df_names else saved_df_names
        st.session_state.pickle_load = True
        st.success("Session state loaded successfully!")

    st.title("Save & Load")


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