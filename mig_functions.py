def standard_sidebar():
    import streamlit as st
    st.sidebar.image('https://agilitypr.news/images/Agility-centered.svg', width=200)
    st.sidebar.header('MIG: Data Cleaning App')
    st.sidebar.caption("v.2.0 alpha")

    # CSS to adjust sidebar
    adjust_nav = """
                            <style>
                            .css-1oe5cao {
                                min-height: 50vh;
                                }
                            .css-1cypcdb .e1fqkh3o11 {
                                max-width: 255px;
                                }
                            </style>
                            """
    # Inject CSS with Markdown
    st.markdown(adjust_nav, unsafe_allow_html=True)


def top_x_by_mentions(df, column_name):
    """Returns top 10 items by mention count"""

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
