import pymongo
import streamlit as st
import pandas as pd
import base64
import numpy as np


# Connect to the MongoDB client
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Access the "stock_calculator" database
db = client["stock_calculator"]

# Access the collections
advertised_report_collection = db["advertised_report"]
all_orders_collection = db["all_orders"]
product_category_collection = db["aekansh_export"]

# Define the fields you want to retrieve
advertised_report_fields = {"Date": 1, "Campaign Name": 1, "Advertised SKU": 1,
                            "7 Day Total Orders (#)": 1, "7 Day Advertised SKU Units (#)": 1,
                            "7 Day Total Sales": 1, "sales-channel": 1, "Spend": 1, "7 Day Advertised SKU Sales": 1}
all_orders_fields = {"purchase-date": 1, "sales-channel": 1, "sku": 1, "quantity": 1, "item-price": 1}

product_category_fields = {"Brand": 1, "Family": 1, "SKU": 1, "Item Title": 1}


# Get all documents from the collections with specific fields
advertised_report_documents = advertised_report_collection.find(projection=advertised_report_fields)
all_orders_documents = all_orders_collection.find(projection=all_orders_fields)
product_category_documents = product_category_collection.find(projection=product_category_fields)

# Convert documents to dataframes
advertised_report_df = pd.DataFrame(list(advertised_report_documents))
all_orders_df = pd.DataFrame(list(all_orders_documents))
product_category_df = pd.DataFrame(list(product_category_documents))


# Apply the function to the 'seller-sku' column
advertised_report_df['Advertised SKU'] = advertised_report_df['Advertised SKU'].apply(lambda x: x.split('_')[0] if x and '_' in x else x)
all_orders_df['sku'] = all_orders_df['sku'].apply(lambda x: x.split('_')[0] if x and '_' in x else x)
product_category_df['SKU'] = product_category_df['SKU'].apply(lambda x: x.split('_')[0] if isinstance(x, str) and '_' in x else x)


# Drop the '_id' columns
advertised_report_df = advertised_report_df.drop(columns=['_id'])
all_orders_df = all_orders_df.drop(columns=['_id'])
product_category_df = product_category_df.drop(columns=['_id'])
product_category_df = product_category_df.drop_duplicates(subset=['SKU'])


# Group by 'Date', 'Advertised SKU', 'Campaign Name', and 'sales-channel' and calculate the sum
advertised_report_df = advertised_report_df.groupby(['Date', 'Advertised SKU', 'Campaign Name', 'sales-channel'], as_index=False).agg({'Spend': 'sum','7 Day Total Orders (#)': 'sum','7 Day Total Sales': 'sum','7 Day Advertised SKU Units (#)': 'sum','7 Day Advertised SKU Sales': 'sum'})

# Change the format of Date in advertised_report_df to dd/mm/yyyy
advertised_report_df['Date'] = pd.to_datetime(advertised_report_df['Date'], errors='coerce')
advertised_report_df.loc[advertised_report_df['Date'].notna(), 'Date'] = advertised_report_df.loc[advertised_report_df['Date'].notna(), 'Date'].dt.strftime('%d/%m/%Y')

all_orders_df['Date'] = pd.to_datetime(all_orders_df['purchase-date'], errors='coerce')
all_orders_df.loc[all_orders_df['Date'].notna(), 'Date'] = all_orders_df.loc[all_orders_df['Date'].notna(), 'Date'].dt.strftime('%d/%m/%Y')

# Drop the original purchase-date column
all_orders_df = all_orders_df.drop(columns=['purchase-date'])

# Rename the 'sku' column to 'Advertised SKU'
all_orders_df.rename(columns={'sku': 'Advertised SKU'}, inplace=True)

# Drop duplicates from advertised_report_df where 'Date', 'Advertised SKU', 'Campaign Name', and 'sales-channel' are the same
advertised_report_df = advertised_report_df.drop_duplicates(subset=['Date', 'Advertised SKU', 'Campaign Name', 'sales-channel'], keep=False)

# Group by 'Date', 'sales-channel', and 'Advertised SKU' and calculate the sum
all_orders_df = all_orders_df.groupby(['Date', 'sales-channel', 'Advertised SKU'], as_index=False).agg({'quantity': 'sum','item-price': 'sum'})

# Ensure that the SKU in product_category_df is of same type as Advertised SKU in the other dataframes
product_category_df['SKU'] = product_category_df['SKU'].astype(str)

# Merge all_orders_df with product_category_df
all_orders_df = pd.merge(all_orders_df, product_category_df, left_on='Advertised SKU', right_on='SKU', how='left')

# Drop the redundant SKU column
all_orders_df.drop('SKU', axis=1, inplace=True)

# Merge advertised_report_df with product_category_df
advertised_report_df = pd.merge(advertised_report_df, product_category_df, left_on='Advertised SKU', right_on='SKU', how='left')

# Drop the redundant SKU column
advertised_report_df.drop('SKU', axis=1, inplace=True)

# Auto detect minimum and maximum dates
min_date = pd.to_datetime(advertised_report_df['Date'], format='%d/%m/%Y').min()
max_date = pd.to_datetime(advertised_report_df['Date'], format='%d/%m/%Y').max()

# Sidebar for data filtering
st.sidebar.subheader('Data Filtering')
min_selected_date = st.sidebar.date_input('Select Minimum Date for First Range', min_value=min_date, max_value=max_date, value=pd.to_datetime(min_date).date())
max_selected_date = st.sidebar.date_input('Select Maximum Date for First Range', min_value=min_date, max_value=max_date, value=pd.to_datetime(max_date).date())
min_selected_date2 = st.sidebar.date_input('Select Minimum Date for Second Range', min_value=min_date, max_value=max_date, value=pd.to_datetime(min_date).date())
max_selected_date2 = st.sidebar.date_input('Select Maximum Date for Second Range', min_value=min_date, max_value=max_date, value=pd.to_datetime(max_date).date())

# Sidebar for SKU filtering
selected_sku = st.sidebar.text_input('Enter SKU (optional)')

# Sidebar for brand and family filtering
selected_brand = st.sidebar.text_input('Enter Brand (optional)')
selected_family = st.sidebar.text_input('Enter Family (optional)')

# Filter data based on selected date range, SKU, brand, and family
filtered_df = advertised_report_df[(pd.to_datetime(advertised_report_df['Date'], format='%d/%m/%Y').dt.date >= min_selected_date) & 
                     (pd.to_datetime(advertised_report_df['Date'], format='%d/%m/%Y').dt.date <= max_selected_date)]

filtered_df2 = advertised_report_df[(pd.to_datetime(advertised_report_df['Date'], format='%d/%m/%Y').dt.date >= min_selected_date2) & 
                      (pd.to_datetime(advertised_report_df['Date'], format='%d/%m/%Y').dt.date <= max_selected_date2)]

if selected_sku:
    filtered_df = filtered_df[filtered_df['Advertised SKU'] == selected_sku]
    filtered_df2 = filtered_df2[filtered_df2['Advertised SKU'] == selected_sku]

if selected_brand:
    filtered_df = filtered_df[filtered_df['Brand'] == selected_brand]
    filtered_df2 = filtered_df2[filtered_df2['Brand'] == selected_brand]

if selected_family:
    filtered_df = filtered_df[filtered_df['Family'] == selected_family]
    filtered_df2 = filtered_df2[filtered_df2['Family'] == selected_family]



# Get unique sales-channel values
sales_channels = advertised_report_df['sales-channel'].unique()

# Sidebar for sales-channel filtering
selected_sales_channel = st.sidebar.selectbox('Select Sales Channel', options=sales_channels)

# Filter data based on selected sales-channel
filtered_df = filtered_df[filtered_df['sales-channel'] == selected_sales_channel]
filtered_df2 = filtered_df2[filtered_df2['sales-channel'] == selected_sales_channel]



# Calculate sum of total spend, 7-day total orders, 7-day total sales, and item price
sum_spend = filtered_df['Spend'].sum()
sum_total_orders = filtered_df['7 Day Total Orders (#)'].sum()
sum_ads_orders = filtered_df['7 Day Advertised SKU Units (#)'].sum()
sum_total_sales = filtered_df['7 Day Total Sales'].sum()
sum_ads_sales = filtered_df['7 Day Advertised SKU Sales'].sum()

sum_spend2 = filtered_df2['Spend'].sum()
sum_total_orders2 = filtered_df2['7 Day Total Orders (#)'].sum()
sum_ads_orders2 = filtered_df2['7 Day Advertised SKU Units (#)'].sum()
sum_total_sales2 = filtered_df2['7 Day Total Sales'].sum()
sum_ads_sales2 = filtered_df2['7 Day Advertised SKU Sales'].sum()

# Filter all_orders_df based on selected date range, SKU, brand, and family
if selected_sku:
    filtered_all_orders_df = all_orders_df[
        (pd.to_datetime(all_orders_df['Date'], format='%d/%m/%Y').dt.date >= min_selected_date) &
        (pd.to_datetime(all_orders_df['Date'], format='%d/%m/%Y').dt.date <= max_selected_date) &
        (all_orders_df['sales-channel'] == selected_sales_channel) &
        (all_orders_df['Advertised SKU'] == selected_sku)
    ]
    filtered_all_orders_df2 = all_orders_df[
        (pd.to_datetime(all_orders_df['Date'], format='%d/%m/%Y').dt.date >= min_selected_date2) &
        (pd.to_datetime(all_orders_df['Date'], format='%d/%m/%Y').dt.date <= max_selected_date2) &
        (all_orders_df['sales-channel'] == selected_sales_channel) &
        (all_orders_df['Advertised SKU'] == selected_sku)
    ]
else:
    filtered_all_orders_df = all_orders_df[
        (pd.to_datetime(all_orders_df['Date'], format='%d/%m/%Y').dt.date >= min_selected_date) &
        (pd.to_datetime(all_orders_df['Date'], format='%d/%m/%Y').dt.date <= max_selected_date) &
        (all_orders_df['sales-channel'] == selected_sales_channel)
    ]
    filtered_all_orders_df2 = all_orders_df[
        (pd.to_datetime(all_orders_df['Date'], format='%d/%m/%Y').dt.date >= min_selected_date2) &
        (pd.to_datetime(all_orders_df['Date'], format='%d/%m/%Y').dt.date <= max_selected_date2) &
        (all_orders_df['sales-channel'] == selected_sales_channel)
    ]

if selected_brand:
    filtered_all_orders_df = filtered_all_orders_df[filtered_all_orders_df['Brand'] == selected_brand]
    filtered_all_orders_df2 = filtered_all_orders_df2[filtered_all_orders_df2['Brand'] == selected_brand]
else:
    filtered_all_orders_df = filtered_all_orders_df.copy()
    filtered_all_orders_df2 = filtered_all_orders_df2.copy()

if selected_family:
    filtered_all_orders_df = filtered_all_orders_df[filtered_all_orders_df['Family'] == selected_family]
    filtered_all_orders_df2 = filtered_all_orders_df2[filtered_all_orders_df2['Family'] == selected_family]
else:
    filtered_all_orders_df = filtered_all_orders_df.copy()
    filtered_all_orders_df2 = filtered_all_orders_df2.copy()


# Calculate sum of total spend, 7-day total orders, 7-day total sales, and item price for all_orders_df

sum_quantity = filtered_all_orders_df['quantity'].sum()
sum_item_price = filtered_all_orders_df['item-price'].sum()

sum_quantity2 = filtered_all_orders_df2['quantity'].sum()
sum_item_price2 = filtered_all_orders_df2['item-price'].sum()


# Create a new dataframe by dropping the "Date" and "Campaign Name" columns
advertise_data_df = filtered_df.drop(["Date", "Campaign Name"], axis=1)

# Group the data by "Advertised SKU" and sum the required columns
advertise_data_df = advertise_data_df.groupby("Advertised SKU").sum().reset_index()


# Create a new dataframe by dropping the "Date" and "Campaign Name" columns
advertise_data_df2 = filtered_df2.drop(["Date", "Campaign Name"], axis=1)

# Group the data by "Advertised SKU" and sum the required columns
advertise_data_df2 = advertise_data_df2.groupby("Advertised SKU").sum().reset_index()

# Merge the dataframes on 'Advertised SKU' using outer join
combined_advertise_df = pd.merge(advertise_data_df, advertise_data_df2, on='Advertised SKU', how='outer')

# Fill NaN values with 'N/A'
combined_advertise_df = combined_advertise_df.fillna('N/A')


# Create a new dataframe by dropping the "Date" and "Campaign Name" columns
all_orders_combined_data_df = filtered_all_orders_df.drop(["Date"], axis=1)

# Group the data by "Advertised SKU" and sum the required columns
all_orders_combined_data_df = all_orders_combined_data_df.groupby("Advertised SKU").sum().reset_index()

# Create a new dataframe by dropping the "Date" and "Campaign Name" columns
all_orders_combined_data2_df = filtered_all_orders_df2.drop(["Date"], axis=1)

# Group the data by "Advertised SKU" and sum the required columns
all_orders_combined_data2_df = all_orders_combined_data2_df.groupby("Advertised SKU").sum().reset_index()

# Merge the dataframes on 'Advertised SKU' using outer join
final_combined_all_order_df = pd.merge(all_orders_combined_data_df, all_orders_combined_data2_df, on='Advertised SKU', how='outer')

# Fill NaN values with 'N/A'
final_combined_all_order_df = final_combined_all_order_df.fillna('N/A')

# Merge the dataframes on 'Advertised SKU' using outer join
combined_advertise_df = pd.merge(advertise_data_df, advertise_data_df2, on='Advertised SKU', how='outer')

# Fill NaN values with 'N/A'
combined_advertise_df = combined_advertise_df.fillna('N/A')

# Ensure that the SKU in product_category_df is of same type as Advertised SKU in the other dataframes
product_category_df['SKU'] = product_category_df['SKU'].astype(str)

# Merge combined_advertise_df with product_category_df
combined_advertise_df = pd.merge(combined_advertise_df, product_category_df, left_on='Advertised SKU', right_on='SKU', how='left')

# Drop the redundant SKU column
combined_advertise_df.drop('SKU', axis=1, inplace=True)

# Merge final_combined_all_order_df with product_category_df
final_combined_all_order_df = pd.merge(final_combined_all_order_df, product_category_df, left_on='Advertised SKU', right_on='SKU', how='left')

# Drop the redundant SKU column
final_combined_all_order_df.drop('SKU', axis=1, inplace=True)





# Display the sum values
col1, col2 = st.columns(2)

with col1:
    st.write("**Date Range 1:**")
    st.write(f"Ad Spend: {sum_spend:.2f}")
    st.write(f"Ads Orders: {sum_total_orders:.2f}")
    st.write(f"Same SKU Orders: {sum_ads_orders:.2f}")
    st.write(f"Advertisements Sales: {sum_total_sales:.2f}")
    st.write(f"Same SKU Sales: {sum_ads_sales:.2f}")
    st.write(f"Total Quantity Ordered: {sum_quantity:.2f}")
    st.write(f"Total Sales: {sum_item_price:.2f}")

with col2:
    st.write("**Date Range 2:**")
    st.write(f"Ad Spend: {sum_spend2:.2f}")
    st.write(f"Ads Orders: {sum_total_orders2:.2f}")
    st.write(f"Same SKU Orders: {sum_ads_orders2:.2f}")
    st.write(f"Advertisements Sales: {sum_total_sales2:.2f}")
    st.write(f"Same SKU Sales: {sum_ads_sales2:.2f}")
    st.write(f"Total Quantity Ordered: {sum_quantity2:.2f}")
    st.write(f"Total Sales: {sum_item_price2:.2f}")





# Add space
st.markdown("<br><br>", unsafe_allow_html=True)
# Display the combined grouped dataframe
st.write(combined_advertise_df)



# Download button for combined_advertise_df
csv_combined_advertise = combined_advertise_df.to_csv(index=False)
b64_combined_advertise = base64.b64encode(csv_combined_advertise.encode()).decode()
href_combined_advertise = f'<a href="data:file/csv;base64,{b64_combined_advertise}" download="combined_advertise.csv">Download Combined Advertise Data</a>'
st.markdown(href_combined_advertise, unsafe_allow_html=True)

# Add space
st.markdown("<br><br>", unsafe_allow_html=True)

# Display the combined grouped dataframe
st.write(final_combined_all_order_df)



# Download button for final_combined_all_order_df
csv_final_combined_all_order = final_combined_all_order_df.to_csv(index=False)
b64_final_combined_all_order = base64.b64encode(csv_final_combined_all_order.encode()).decode()
href_final_combined_all_order = f'<a href="data:file/csv;base64,{b64_final_combined_all_order}" download="final_combined_all_order.csv">Download Final Combined All Order Data</a>'
st.markdown(href_final_combined_all_order, unsafe_allow_html=True)

