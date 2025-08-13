import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2 # Used for the general connection errors

# Import functions from database_ops.py
# Make sure database_ops.py is in the same directory
from database import connect_db, execute_query, add_provider, \
                         get_all_food_listings, update_claim_status, delete_food_listing
                      # Include this for initial setup, but run once

# --- Configuration ---
st.set_page_config(
    page_title="Food Wastage & Donation Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Database Initialization (Run once on app start) ---
# IMPORTANT: This block is for initial setup and populating the DB from CSVs.
# You typically run this ONCE when you set up your database for the first time.
# If your DB is already populated, you can comment out or remove this `init_db_and_data` call
# to avoid reprocessing CSVs on every app run.

# --- Utility Function to fetch data and cache it ---
 # Cache data for 1 hour
def get_data_for_display(query, params=None):
    """Fetches data from the database using execute_query and caches it."""
    return execute_query(query, params)

# --- Title and Introduction ---
st.title(" Food Wastage & Donation Management")
st.write("Understand food wastage trends, manage donation records, and facilitate distribution.")

# --- Tabbed Navigation ---
tab1, tab2, tab3 = st.tabs(["Dashboard", "SQL Analysis & Trends", "Admin Operations (CRUD)"])

# --- Tab 1: Dashboard & Filtering ---
with tab1:
# Fetch unique values for filter dropdowns (these can also be cached)
    @st.cache_data
    def get_filter_options():
        cities = get_data_for_display("SELECT DISTINCT City FROM providers ORDER BY City;").iloc[:, 0].tolist()
        # For provider names, join providers and food_listings to get only providers with listings
        providers_with_listings_query = """
        SELECT DISTINCT p.Name
        FROM providers p
        JOIN food fl ON p.Provider_ID = fl.Provider_ID
        ORDER BY p.Name;
        """
        providers_list = get_data_for_display(providers_with_listings_query).iloc[:, 0].tolist()
        provider_type=get_data_for_display("select distinct type from providers order by type;").iloc[:, 0].tolist()
        receiver_type=get_data_for_display("select distinct type from receivers order by type;").iloc[:, 0].tolist()
        food_types = get_data_for_display("SELECT DISTINCT Food_Type FROM food ORDER BY Food_Type;").iloc[:, 0].tolist()
        meal_types = get_data_for_display("SELECT DISTINCT Meal_Type FROM food ORDER BY Meal_Type;").iloc[:, 0].tolist()
        return ["All"] + cities, ["All"] + providers_list,["All"] + provider_type,["All"] + receiver_type, ["All"] + food_types, ["All"] + meal_types
    cities, providers_list, provider_type,receiver_type,food_types, meal_types = get_filter_options()

    # --- Sidebar for Filters ---
    st.sidebar.header("Filter Available Food")
    selected_city = st.sidebar.selectbox("City:", cities)
    selected_provider: None = st.sidebar.selectbox("Provider:", providers_list)
    selected_provider_type = st.sidebar.selectbox("Provider Type:", provider_type)
    selected_receiver_type = st.sidebar.selectbox("Receiver Type:", receiver_type)
    selected_food_type = st.sidebar.selectbox("Food Type:", food_types)
    selected_meal_type = st.sidebar.selectbox("Meal Type:", meal_types)
                             
# --- Build Filtered Query ---
    filter_query = """
    SELECT
        fl.Food_Name,
        fl.Quantity,
        fl.Expiry_Date,
        fl.Food_Type,
        fl.Meal_Type,
        p.Name AS Provider_Name,
        p.Type AS Provider_Type,
        p.City AS Provider_City,
        p.Contact AS Provider_Contact
        r.receiver_type as receiver_type
    FROM food fl
    JOIN providers p ON fl.Provider_ID = p.Provider_ID
    join claims c ON c.food_id=fl.food_id
    join receivers r on r.receiver_id=c.receiver_id
    WHERE fl.Expiry_Date >= c.timestamp -- Only show unexpired food
    """
    query_params = []

    if selected_city != "All":
        filter_query += " AND p.City = %s"
        query_params.append(selected_city)
    if selected_provider != "All":
        filter_query += " AND p.Name = %s"
        query_params.append(selected_provider)
    if selected_provider_type != "All:":
        filter_query += " AND p.type = %s"
        query_params.append(selected_provider_type)
    if selected_receiver_type != "All":
        filter_query += " AND r.receiver_type = %s"
        query_params.append(selected_receiver_type)
    if selected_food_type != "All":
        filter_query += " AND fl.Food_Type = %s"
        query_params.append(selected_food_type)
    if selected_meal_type != "All":
        filter_query += " AND fl.Meal_Type = %s"
        query_params.append(selected_meal_type)

    filter_query += " ORDER BY fl.Expiry_Date ASC, p.Name, fl.Food_Name;"

    filtered_listings_df = get_data_for_display(filter_query, tuple(query_params) if query_params else None)

    if not filtered_listings_df.empty:
        st.subheader("Filtered Food Listings")
        st.dataframe(filtered_listings_df, use_container_width=True)

        st.subheader("Contact Information for Providers")
        contact_df = filtered_listings_df[['Provider_Name', 'Provider_Type', 'Provider_City', 'Provider_Contact']].drop_duplicates()
        if not contact_df.empty:
            st.dataframe(contact_df, use_container_width=True)
        else:
            st.info("No provider contact information available for these filtered listings.")

    else:
        st.info("No food listings available matching your criteria.")

    st.markdown("---")
    st.subheader("Key Performance Indicators (KPIs)")

    # Example KPIs - fetching values for st.metric
    total_food_available_query = "SELECT SUM(Quantity) FROM food;"
    total_claims_query = "SELECT COUNT(Claim_ID) FROM claims;"
    total_providers_query = "SELECT COUNT(Provider_ID) FROM providers;"
    most_claimed_mealtype_query = "SELECT MEAL_TYPE FROM(SELECT A.MEAL_TYPE,COUNT(*) AS CLAIMED FROM FOOD AS A JOIN CLAIMS AS B ON A.FOOD_ID=B.FOOD_ID GROUP BY 1 ORDER BY 2 DESC LIMIT 1);"
    city_highest_food_query="SELECT LOCATION AS CITY, COUNT(*) AS LISTING FROM FOOD GROUP BY 1 ORDER BY 2 DESC LIMIT 1;"

    total_food_available = get_data_for_display(total_food_available_query).iloc[0, 0] if not get_data_for_display(total_food_available_query).empty else 0
    total_claims = get_data_for_display(total_claims_query).iloc[0, 0] if not get_data_for_display(total_claims_query).empty else 0
    total_providers = get_data_for_display(total_providers_query).iloc[0, 0] if not get_data_for_display(total_providers_query).empty else 0
    most_mealtype = get_data_for_display(most_claimed_mealtype_query).iloc[0,0] if not get_data_for_display(most_claimed_mealtype_query).empty else 0
    city_highest_food = get_data_for_display(city_highest_food_query).iloc[0,0] if not get_data_for_display(city_highest_food_query).empty else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric(label="Total Food Available", value=f"{int(total_food_available):,}")
    with col2:
        st.metric(label="Total Claims Made", value=f"{int(total_claims):,}")
    with col3:
        st.metric(label="Total Registered Providers", value=f"{int(total_providers):,}")
    with col4:
        st.metric(label="Most claimed Meal type", value=f"{str(most_mealtype):}")
    with col5:
        st.metric(label="City with highest food lisings", value=f"{str(city_highest_food):}")


    st.markdown("---")
    st.subheader("Visualizing Trends")

    # Example: Bar chart for food contribution by provider type
    provider_contribution_query = """
    SELECT p.Type AS Provider_Type, SUM(fl.Quantity) AS Total_Food_Quantity
    FROM food fl
    JOIN providers p ON fl.Provider_ID = p.Provider_ID
    GROUP BY p.Type
    ORDER BY Total_Food_Quantity DESC;
    """
    provider_contribution_df = get_data_for_display(provider_contribution_query)
    col1,col2=st.columns(2)
    with col1:
        if not provider_contribution_df.empty:
           st.write("#### Food Quantity by Provider Type")
           st.bar_chart(provider_contribution_df.set_index('provider_type'))
        else:
           st.info("No data to display for provider contribution.")

    # Example: Pie chart (or bar) for claim status percentage
    claim_status_query = """
    SELECT Status, COUNT(*) AS Num_Claims
    FROM claims
    GROUP BY Status;
    """
    claim_status_df = get_data_for_display(claim_status_query)
    with col2:
        if not claim_status_df.empty:
           st.write("#### Claim Status Distribution")
        # Streamlit's bar_chart is simpler for quick display
           fig = px.pie(claim_status_df, values='num_claims', names='status')
           st.plotly_chart (fig, use_container_width=True)
           st.caption("Completed, Pending, and Cancelled Claims.")
        else:
           st.info("No claim status data to display.")

    date_trend_query = """ select extract(day from timestamp) as date,
                        count(*) as claimed from claims group by date"""
    date_trend_df = get_data_for_display(date_trend_query)
    if not date_trend_df.empty:
        st.write("#### Date Trend")
        st.line_chart(date_trend_df.set_index('date'))
        st.caption("Date Trend")
    else:
        st.info("No date trend data to display.")

# --- Tab 2: SQL Analysis & Trends ---
with tab2:
    st.header("Deep Dive: SQL Query Results & Analysis")
    st.write("Explore detailed insights from the 15 pre-defined SQL queries.")

    # Define all your SQL queries with titles and optional parameters
    # The queries are the same as provided in our previous discussion
    queries_to_display = [
        ("1. Providers & Receivers per City", """
        SELECT City,
               COUNT(DISTINCT Provider_ID) AS Num_Providers,
               COUNT(DISTINCT Receiver_ID) AS Num_Receivers
        FROM providers
        FULL OUTER JOIN receivers USING (City)
        GROUP BY City
        ORDER BY City;
        """),
        ("2. Food Contribution by Provider Type", """
        SELECT p.Type AS Provider_Type,
               SUM(fl.Quantity) AS Total_Food_Quantity
        FROM food fl
        JOIN providers p ON fl.Provider_ID = p.Provider_ID
        GROUP BY p.Type
        ORDER BY Total_Food_Quantity DESC;
        """),
        ("3. Contact Info of Providers in a Specific City ", """
        SELECT City,Name,address,Contact
        FROM providers;
        """), # Example parameter. You can make this dynamic with st.selectbox/st.text_input
        ("4. Receivers Claimed Most Food", """
        SELECT r.Name AS Receiver_Name,
               SUM(fl.Quantity) AS Total_Food_Claimed
        FROM claims c
        JOIN food fl ON c.Food_ID = fl.Food_ID
        JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
        WHERE c.Status = 'Completed'
        GROUP BY r.Name
        ORDER BY Total_Food_Claimed DESC
        LIMIT 10;
        """),
        ("5. Total Quantity of Unexpired Food Available", """
        SELECT SUM(f.Quantity) AS Total_Available_Food
        FROM food f
        WHERE f.Expiry_Date >= (select timestamp::date from claims);
        """),
        ("6. City with Highest Number of Food Listings", """
        SELECT Location AS City,
               COUNT(Food_ID) AS Number_Of_Listings
        FROM food
        GROUP BY Location
        ORDER BY Number_Of_Listings DESC
        LIMIT 1;
        """),
        ("7. Most Commonly Available Food Types", """
        SELECT Food_Type,
               COUNT(Food_ID) AS Number_Of_Listings
        FROM food
        GROUP BY Food_Type
        ORDER BY Number_Of_Listings DESC;
        """),
        ("8. How many food claims have been made for each food item?", """
        SELECT fl.Food_Name,
               COUNT(c.Claim_ID) AS Number_Of_Claims
        FROM food fl
        LEFT JOIN claims c ON fl.Food_ID = c.Food_ID
        GROUP BY fl.Food_Name
        ORDER BY Number_Of_Claims DESC;
        """),
        ("9. Which provider has had the highest number of successful food claims?", """
        SELECT p.Name AS Provider_Name,
               COUNT(c.Claim_ID) AS Number_Of_Successful_Claims
        FROM providers p
        JOIN food fl ON p.Provider_ID = fl.Provider_ID
        JOIN claims c ON fl.Food_ID = c.Food_ID
        WHERE c.Status = 'Completed'
        GROUP BY p.Name
        ORDER BY Number_Of_Successful_Claims DESC
        LIMIT 1;
        """),
        ("10. What percentage of food claims are completed vs. pending vs. canceled?", """
        SELECT Status,
               COUNT(*) AS Num_Claims,
               ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM claims), 2) AS Percentage
        FROM claims
        GROUP BY Status
        ORDER BY Num_Claims DESC;
        """),
        ("11. What is the average quantity of food claimed per receiver?", """
        SELECT AVG(Total_Food_Claimed) AS Average_Quantity_Claimed_Per_Receiver
        FROM (
            SELECT r.Receiver_ID, SUM(fl.Quantity) AS Total_Food_Claimed
            FROM claims c
            JOIN food fl ON c.Food_ID = fl.Food_ID
            JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
            WHERE c.Status = 'Completed'
            GROUP BY r.Receiver_ID
        ) AS ReceiverClaims;
        """),
        ("12. Which meal type (breakfast, lunch, dinner, snacks) is claimed the most?", """
        SELECT fl.Meal_Type,
               COUNT(c.Claim_ID) AS Number_Of_Claims
        FROM claims c
        JOIN food fl ON c.Food_ID = fl.Food_ID
        WHERE c.Status = 'Completed'
        GROUP BY fl.Meal_Type
        ORDER BY Number_Of_Claims DESC;
        """),
        ("13. What is the total quantity of food donated by each provider?", """
        SELECT p.Name AS Provider_Name,
               SUM(fl.Quantity) AS Total_Donated_Quantity
        FROM providers p
        JOIN food fl ON p.Provider_ID = fl.Provider_ID
        GROUP BY p.Name
        ORDER BY Total_Donated_Quantity DESC;
        """),
        ("14. List all food items expiring in the next 7 days", """
        SELECT Food_Name, Quantity, Expiry_Date, p.Name as Provider_Name, p.City as Provider_City
        FROM food fl
        JOIN providers p ON fl.Provider_ID = p.Provider_ID
        WHERE Expiry_Date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
        ORDER BY Expiry_Date ASC;
        """),
        ("15. Show unfulfilled claims (pending claims) with food and receiver details", """
        SELECT c.Claim_ID, fl.Food_Name, fl.Quantity, r.Name AS Receiver_Name, r.Contact AS Receiver_Contact, c.Timestamp
        FROM claims c
        JOIN food fl ON c.Food_ID = fl.Food_ID
        JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
        WHERE c.Status = 'Pending'
        ORDER BY c.Timestamp DESC;
        """)
    ]

    for i, (title, query, *params) in enumerate(queries_to_display):
        with st.expander(f"Query {i+1}: {title}"):
            st.code(query, language='sql')
            # Handle parameterized query if needed
            if params:
                df_result = get_data_for_display(query, params[0])
            else:
                df_result = get_data_for_display(query)

            if not df_result.empty:
                st.dataframe(df_result, use_container_width=True)
                # Optional: Add a simple chart for some queries
                if "Quantity" in df_result.columns or "Num_Claims" in df_result.columns or "Percentage" in df_result.columns:
                    try:
                        st.write("Visualisation:")
                        if "Provider_Type" in df_result.columns and "Total_Food_Quantity" in df_result.columns:
                            st.bar_chart(df_result.set_index('Provider_Type')['Total_Food_Quantity'])
                        elif "Status" in df_result.columns and "Num_Claims" in df_result.columns:
                             st.bar_chart(df_result.set_index('Status')['Num_Claims'])
                        elif "Food_Type" in df_result.columns and "Number_Of_Listings" in df_result.columns:
                             st.bar_chart(df_result.set_index('Food_Type')['Number_Of_Listings'])
                        # Add more conditions for other charts based on query results
                    except Exception as e:
                        st.write(f"Could not generate chart for this query: {e}")
            else:
                st.info("No data found for this query.")

# --- Tab 3: Admin Operations (CRUD) ---
with tab3:
    st.header("Admin Operations: Add, Update, Delete Records")
    st.write("Manage providers, food listings, receivers, and claims directly.")

    crud_action = st.selectbox(
        "Select an operation:",
        ["Add Provider", "Add Food Listing", "Update Claim Status", "Delete Food Listing", "View All Tables"]
    )

    if crud_action == "Add Provider":
        st.subheader("➕ Add New Food Provider")
        with st.form("add_provider_form", clear_on_submit=True):
            name = st.text_input("Provider Name", key="add_provider_name")
            provider_type = st.selectbox("Provider Type", ["Restaurant", "Grocery Store", "Supermarket", "Caterer", "Other"], key="add_provider_type")
            address = st.text_input("Address", key="add_provider_address")
            city = st.text_input("City", key="add_provider_city")
            contact = st.text_input("Contact (Phone)", key="add_provider_contact")
            submitted = st.form_submit_button("Add Provider")
            if submitted:
                if name and provider_type and city:
                    add_provider(name, provider_type, address, city, contact)
                    st.success(f"Provider '{name}' added successfully!")
                else:
                    st.error("Name, Type, and City are required fields for a new provider.")

    elif crud_action == "Add Food Listing":
        st.subheader("➕ Add New Food Listing")
        # Fetch existing providers for dropdown
        providers_df_for_select = get_data_for_display("SELECT provider_id, name FROM providers ORDER BY name;")
        provider_options = providers_df_for_select.apply(lambda row: f"{row['provider_id']} - {row['Name']}", axis=1).tolist()

        with st.form("add_food_listing_form", clear_on_submit=True):
            food_name = st.text_input("Food Item Name", key="add_food_name")
            quantity = st.number_input("Quantity", min_value=1, value=1, step=1, key="add_quantity")
            expiry_date = st.date_input("Expiry Date", key="add_expiry_date")
            selected_provider_option = st.selectbox("Select Provider", provider_options, key="add_food_provider_id")
            food_type = st.selectbox("Food Type", ["Vegetarian", "Non-Vegetarian", "Vegan", "Gluten-Free", "Dairy-Free", "Mixed"], key="add_food_type")
            meal_type = st.selectbox("Meal Type", ["Breakfast", "Lunch", "Dinner", "Snacks", "Other"], key="add_meal_type")

            submitted = st.form_submit_button("Add Food Listing")
            if submitted:
                if food_name and quantity and expiry_date and selected_provider_option:
                    provider_id = int(selected_provider_option.split(' - ')[0])
                    # Need to get provider_type and location from the selected provider_id
                    provider_details = get_data_for_display("SELECT type, city FROM providers WHERE provider_id = %s;", (provider_id,)).iloc[0]
                    provider_type_from_db = provider_details['type']
                    location_from_db = provider_details['city']

                    # Manually execute INSERT for food_listings
                    insert_food_listing_query = """
                    INSERT INTO food_listings (food_name, quantity, expiry_date, provider_id, provider_type, location, food_type, meal_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    execute_query(insert_food_listing_query, (
                        food_name, quantity, expiry_date, provider_id, provider_type_from_db, location_from_db, food_type, meal_type
                    ))
                    st.success(f"Food listing '{food_name}' added successfully!")
                else:
                    st.error("Food Name, Quantity, Expiry Date, and Provider are required.")

    elif crud_action == "Update Claim Status":
        st.subheader("✏️ Update Claim Status")
        # Fetch existing claims for dropdown
        claims_data = get_data_for_display("""
        SELECT c.claim_id, fl.food_name, r.name AS Receiver_Name, c.status, c.timestamp
        FROM claims c
        JOIN food fl ON c.food_id = fl.food_id
        JOIN receivers r ON c.receiver_id = r.Receiver_id
        ORDER BY c.claim_id DESC;
        """)

        if not claims_data.empty:
            claim_options = claims_data.apply(lambda row: f"Claim ID: {row['Claim_ID']} - {row['Food_Name']} to {row['Receiver_Name']} (Status: {row['Status']})", axis=1).tolist()
            selected_claim_str = st.selectbox("Select Claim to Update:", claim_options)

            if selected_claim_str:
                selected_claim_id = int(selected_claim_str.split(' - ')[0].replace('Claim ID: ', ''))
                current_status = claims_data[claims_data['Claim_ID'] == selected_claim_id]['Status'].iloc[0]
                st.write(f"Current Status: **{current_status}**")
                new_status = st.selectbox("New Status:", ["Pending", "Completed", "Cancelled"], index=["Pending", "Completed", "Cancelled"].index(current_status))
                if st.button("Update Claim Status"):
                    update_claim_status(selected_claim_id, new_status)
                    st.success(f"Claim {selected_claim_id} status updated to '{new_status}'.")
                    st.rerun() # Rerun to refresh the dropdown display
        else:
            st.info("No claims found to update.")

    elif crud_action == "Delete Food Listing":
        st.subheader("🗑️ Delete Food Listing")
        food_listings_for_delete = get_data_for_display("SELECT Food_ID, Food_Name, Quantity FROM food_listings ORDER BY Food_ID DESC;")
        if not food_listings_for_delete.empty:
            food_options = food_listings_for_delete.apply(lambda row: f"{row['Food_ID']} - {row['Food_Name']} (Qty: {row['Quantity']})", axis=1).tolist()
            selected_food_option = st.selectbox("Select Food Listing to Delete:", food_options)
            if selected_food_option:
                food_id_to_delete = int(selected_food_option.split(' ')[0])
                st.warning(f"Deleting Food ID {food_id_to_delete} will also delete any associated claims due to foreign key constraints.")
                if st.button(f"Confirm Delete Listing {food_id_to_delete}"):
                    delete_food_listing(food_id_to_delete)
                    st.success(f"Food listing {food_id_to_delete} and associated claims deleted.")
                    st.rerun() # Rerun to refresh the dropdown
        else:
            st.info("No food listings found to delete.")

    elif crud_action == "View All Tables":
        st.subheader("📊 View All Data Tables")
        st.write("Select a table to view its full contents.")
        table_name = st.selectbox("Choose a table:", ["providers", "receivers", "food_listings", "claims"])
        all_data_df = get_data_for_display(f"SELECT * FROM {table_name};")
        if not all_data_df.empty:
            st.dataframe(all_data_df, use_container_width=True)
        else:
            st.info(f"No data in the '{table_name}' table.")