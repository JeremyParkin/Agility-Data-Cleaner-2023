import mig_functions as mig
import dill
import base64
import streamlit as st
import pandas as pd
import io
from datetime import datetime


# Set Streamlit configuration
st.set_page_config(page_title="MIG Data Processing App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png",
                   layout="wide")

# Sidebar configuration
mig.standard_sidebar()

dt_string = datetime.now().strftime("%Y-%m-%d-%H-%M")


df_names = st.session_state.df_names  # List of DataFrames in the session state



def save_session_state():
    # Manually copy necessary items from session state
    session_data = {key: value for key, value in st.session_state.items() if key not in st.session_state.df_names}

    # Serialize all DataFrames in the session state
    for df_name in st.session_state.df_names:
        if df_name in st.session_state and not st.session_state[df_name].empty:  # Check if DataFrame is not empty
            buffer = io.StringIO()
            st.session_state[df_name].to_csv(buffer, index=False)
            session_data[df_name] = buffer.getvalue()

    # Serialize the session state
    serialized_data = dill.dumps(session_data)

    # Encode the serialized data for downloading
    b64 = base64.b64encode(serialized_data).decode()

    # Generate a download link
    href = f'<a href="data:file/pkl;base64,{b64}" download="{st.session_state.client_name} - {dt_string}.pkl">Download Session File</a>'


    return href

def load_session_state(uploaded_file):
    if uploaded_file is not None:
        # Read the uploaded file
        session_data = uploaded_file.getvalue()

        # Deserialize the session state
        deserialized_data = dill.loads(session_data)

        # List of columns to be converted to integers
        integer_columns = ['Impressions', 'Audience Reach', 'Domain Authority']  # Add your column names here

        # Convert CSV strings back to DataFrames
        for df_name in st.session_state.df_names:
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
                    deserialized_data[df_name] = pd.DataFrame()  # Initialize an empty DataFrame

        # Update the session state
        st.session_state.update(deserialized_data)
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
#
# st.info("""**SAVE** your current data-processing session to a downloadable .pkl file
#         \n**LOAD** a previously saved data-processing session from a downloaded .pkl file""")


    # When this button is clicked, the save_session_state function will be executed
    if st.button("Generate Session File to Download"):
        placeholder = st.empty()
        placeholder.info("Processing... please wait.")
        # Generate the download link (or any other way you handle the saving)
        href = save_session_state()

        st.markdown(href, unsafe_allow_html=True)
        placeholder.empty()

    st.divider()

st.header("LOAD")
st.info("**LOAD** a previously saved data-processing session from a downloaded .pkl file")

uploaded_file = st.file_uploader("Restore a Previous Session", type="pkl", label_visibility="hidden")
if uploaded_file is not None:
    load_session_state(uploaded_file)