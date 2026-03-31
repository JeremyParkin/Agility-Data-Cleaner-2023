def standard_sidebar():
    import sys
    import platform
    import streamlit as st
    import importlib.metadata as metadata
    st.sidebar.image('https://jeremyparkin.com/wp-content/uploads/2025/05/AG_main-light-grey.png', width=180)

    st.sidebar.subheader('MIG Data Processing App')
    st.sidebar.caption("Version: March 2026")

    # CSS to adjust sidebar
    adjust_nav = """
                            <style>
                            
                            .eczjsme9, .st-emotion-cache-1wqrzgl {
                                overflow: visible !important;
                                max-width: 250px !important;
                                }
                                
                            .eczjsme16 {
                                min-height: 375px !important;
                                }
                            
                            .st-emotion-cache-a8w3f8 {
                                overflow: visible !important;
                                }
                            .st-emotion-cache-1cypcdb {
                                max-width: 250px !important;
                                }
                           
                            </style>
                            """
    # Inject CSS with Markdown
    st.markdown(adjust_nav, unsafe_allow_html=True)

    # Add link to submit bug reports and feature requests
    st.sidebar.markdown(
        "[App Feedback](https://forms.office.com/Pages/ResponsePage.aspx?id=GvcJkLbBVUumZQrrWC6V07d2jCu79C5FsfEZJPZEfZxUNVlIVDRNNVBQVEgxQVFXNEM5VldUMkpXNS4u)")

    with st.sidebar.expander("Environment info"):
        st.text(f"Python: {sys.version.split()[0]}")
        st.text(f"Platform: {platform.platform()}")

        package_names = ["streamlit", "pandas", "numpy", "openpyxl", "xlsxwriter", "openai", "titlecase", "requests",
                         "unidecode", "deep-translator", "dill", "jinja2", "streamlit-tags"]
        # streamlit = "~=1.55.0"
        # pandas = "~=2.3.3"
        # openai = "~=2.6.1"
        # titlecase = "~=2.4.1"
        # requests = "~=2.32.5"
        # unidecode = "~=1.4.0"
        # xlsxwriter = "~=3.2.9"
        # deep - translator = "~=1.11.4"
        # dill = "~=0.4.0"
        # openpyxl = "~=3.1.5"
        # jinja2 = "~=3.1.6"
        # streamlit - tags = "~=1.2.8"

        for pkg in package_names:
            try:
                st.text(f"{pkg}: {metadata.version(pkg)}")
            except metadata.PackageNotFoundError:
                st.text(f"{pkg}: not installed")

def top_x_by_mentions(df, column_name):
    """Returns top 10 items by mention count"""
    if not df[column_name].notna().any():
        # If all values in the column are null, return an empty dataframe
        return
    top10 = df[[column_name, 'Mentions']].groupby(
        by=[column_name]).sum().sort_values(
        ['Mentions'], ascending=False)
    top10 = top10.rename(columns={"Mentions": "Hits"})

    return top10.head(10)


def fix_author(df, headline_text, new_author):
    """Updates all authors for a given headline"""
    df.loc[df["Headline"] == headline_text, "Author"] = new_author


def headline_authors(df, headline_text):
    """Returns the various authors for a given headline"""
    headline_authors = (df[df.Headline == headline_text].Author.value_counts().reset_index())
    return headline_authors



def format_number(num):
    if num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.1f} B"
    elif num >= 1_000_000:
        return f"{num / 1_000_000:.1f} M"
    elif num >= 1_000:
        return f"{num / 1_000:.1f} K"
    else:
        return str(num)


def require_standard_pipeline():
    import streamlit as st

    if not st.session_state.get("upload_step", False):
        st.error("Please upload a CSV before trying this step.")
        st.stop()

    if not st.session_state.get("standard_step", False):
        st.error("Please run the Standard Cleaning before trying this step.")
        st.stop()