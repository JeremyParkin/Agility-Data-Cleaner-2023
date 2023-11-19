import streamlit as st
import pandas as pd
import mig_functions as mig
import io
import warnings
warnings.filterwarnings('ignore')
st.set_page_config(layout="wide", page_title="MIG Data Cleaning App",
                   page_icon="https://www.agilitypr.com/wp-content/uploads/2018/02/favicon-192.png")

mig.standard_sidebar()

st.title('Download')

if not st.session_state.upload_step:
    st.error('Please upload a CSV before trying this step.')
elif not st.session_state.standard_step:
    st.error('Please run the Standard Cleaning before trying this step.')
else:
    # export_name = st.session_state.export_name

    traditional = st.session_state.df_traditional
    social = st.session_state.df_social
    auth_outlet_table = st.session_state.auth_outlet_table
    top_stories = st.session_state.added_df

    # Tag exploder
    if "Tags" in st.session_state.df_traditional:
        traditional['Tags'] = traditional['Tags'].astype(str)  # needed if column there but all blank
        traditional = traditional.join(traditional["Tags"].str.get_dummies(sep=",").astype('category'), how='left', rsuffix=' (tag)')

    if "Tags" in social:
        social['Tags'] = social['Tags'].astype(str)  # needed if column there but all blank
        social = social.join(social["Tags"].str.get_dummies(sep=",").astype('category'), how='left', rsuffix=' (tag)')

    with st.form("my_form_download"):
        st.subheader("Generate your cleaned data workbook")
        submitted = st.form_submit_button("Go!", type="primary")
        if submitted:
            with st.spinner('Building workbook now...'):

                output = io.BytesIO()
                writer = pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='yyyy-mm-dd')

                workbook = writer.book
                cleaned_dfs = []
                cleaned_sheets = []

                # Add some cell formats.
                number_format = workbook.add_format({'num_format': '#,##0'})
                currency_format = workbook.add_format({'num_format': '$#,##0'})

                if len(traditional) > 0:
                    traditional = traditional.rename(columns={'AVE': st.session_state.ave_col[0]})
                    traditional = traditional.sort_values(by=['Impressions'], ascending=False)
                    traditional.to_excel(writer, sheet_name='CLEAN TRAD', startrow=1, header=False, index=False)
                    worksheet1 = writer.sheets['CLEAN TRAD']
                    worksheet1.set_tab_color('black')
                    cleaned_dfs.append((traditional, worksheet1))
                    cleaned_sheets.append(worksheet1)

                if len(social) > 0:
                    social = social.rename(columns={'AVE': st.session_state.ave_col[0]})
                    social = social.sort_values(by=['Impressions'], ascending=False)
                    social.to_excel(writer, sheet_name='CLEAN SOCIAL', startrow=1, header=False, index=False)
                    worksheet2 = writer.sheets['CLEAN SOCIAL']
                    worksheet2.set_tab_color('black')
                    cleaned_dfs.append((social, worksheet2))
                    cleaned_sheets.append(worksheet2)

                if len(auth_outlet_table) > 0:
                    authors = auth_outlet_table.sort_values(by=['Mentions', 'Impressions'], ascending=False)
                    authors.to_excel(writer, sheet_name='Authors', header=True, index=False)
                    worksheet5 = writer.sheets['Authors']
                    worksheet5.set_tab_color('green')
                    worksheet5.set_default_row(22)
                    worksheet5.set_column('A:A', 30, None)  # author
                    worksheet5.set_column('C:C', 12, None)  # mentions
                    worksheet5.set_column('D:D', 15, number_format)  # impressions
                    worksheet5.set_column('B:B', 35, None)  # outlet
                    worksheet5.freeze_panes(1, 0)
                    cleaned_dfs.append((authors, worksheet5))

                if len(top_stories) > 0:
                    top_stories = top_stories.sort_values(by=['Mentions', 'Impressions'], ascending=False)
                    top_stories.to_excel(writer, sheet_name='Top Stories', header=True, index=False)
                    worksheet6 = writer.sheets['Top Stories']
                    worksheet6.set_tab_color('green')
                    worksheet6.set_column('A:A', 35, None)  # headline
                    worksheet6.set_column('B:B', 12, None)  # date
                    worksheet6.set_column('C:C', 12, number_format)  # mentions
                    worksheet6.set_column('D:D', 12, number_format)  # impressions
                    worksheet6.set_column('E:E', 20, None)  # outlet
                    worksheet6.set_column('F:F', 15, None)  # url
                    worksheet6.set_column('G:G', 15, None)  # type
                    worksheet6.set_column('H:H', 15, None)  # snippet
                    worksheet6.set_column('I:I', 40, None)  # summary
                    worksheet6.set_column('J:J', 40, None)  # sentiment
                    worksheet6.freeze_panes(1, 0)
                    cleaned_dfs.append((top_stories, worksheet6))



                if len(st.session_state.df_dupes) > 0:
                    st.session_state.df_dupes = st.session_state.df_dupes.rename(columns={
                        'AVE': st.session_state.ave_col[0]})
                    st.session_state.df_dupes.to_excel(writer, sheet_name='DLTD DUPES', header=True, index=False)
                    worksheet3 = writer.sheets['DLTD DUPES']
                    worksheet3.set_tab_color('#c26f4f')
                    cleaned_dfs.append((st.session_state.df_dupes, worksheet3))
                    cleaned_sheets.append(worksheet3)

                st.session_state.df_untouched = st.session_state.df_untouched.rename(columns={
                    'AVE': st.session_state.ave_col[0]})
                st.session_state.df_untouched.drop(["Mentions"], axis=1, inplace=True, errors='ignore')
                st.session_state.df_untouched.to_excel(writer, sheet_name='RAW', header=True, index=False)
                worksheet4 = writer.sheets['RAW']
                worksheet4.set_tab_color('#c26f4f')
                cleaned_dfs.append((st.session_state.df_untouched, worksheet4))

                for clean_df in cleaned_dfs:
                    (max_row, max_col) = clean_df[0].shape
                    column_settings = [{'header': column} for column in clean_df[0].columns]
                    clean_df[1].add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

                # Add the Excel table structure. Pandas will add the data.
                for sheet in cleaned_sheets:
                    sheet.set_default_row(22)
                    sheet.set_column('A:A', 12, None)  # datetime
                    sheet.set_column('B:B', 22, None)  # outlet
                    sheet.set_column('C:C', 10, None)  # type
                    sheet.set_column('G:G', 12, None)  # author
                    sheet.set_column('E:E', 0, None)  # mentions
                    sheet.set_column('F:F', 12, number_format)  # impressions
                    sheet.set_column('H:H', 40, None)  # headline
                    sheet.set_column('U:U', 12, currency_format)  # AVE
                    sheet.freeze_panes(1, 0)

                workbook.close()

    if submitted:
        export_name = f"{st.session_state.export_name} - clean_data.xlsx"
        st.download_button('Download', output, file_name=export_name, type="primary")
