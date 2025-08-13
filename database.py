import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # Needed for CREATE DATABASE
import pandas as pd
from sqlalchemy import create_engine # For pandas.to_sql

# --- 1. Database Connection Details ---
# IMPORTANT: Replace these with your actual PostgreSQL credentials.
# For deployment, these should ideally come from environment variables or Streamlit secrets.
DB_NAME = "Wastage"   # The name of the database you want to connect to
DB_USER = "postgres"      # Your PostgreSQL username (e.g., "postgres")
DB_PASS = "1234"      # Your PostgreSQL password
DB_HOST = "localhost"          # 'localhost' if running on the same machine, otherwise the IP address or hostname
DB_PORT = "5432"               # Default PostgreSQL port

# --- 2. Function to Connect to the Database ---
def connect_db(db_name=DB_NAME):
    """
    Establishes a connection to the specified PostgreSQL database.
    Args:
        db_name (str): The name of the database to connect to.
                       Defaults to DB_NAME for regular operations.
    Returns:
        psycopg2.connection or None: The connection object if successful, None otherwise.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        # print(f"Successfully connected to database: {db_name}") # Optional: for debugging
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database '{db_name}': {e}")
        print(f"Please check your DB_HOST, DB_PORT, DB_USER, DB_PASS, and ensure PostgreSQL is running.")
    except Exception as e:
        print(f"An unexpected error occurred during database connection: {e}")
    return conn





# --- 6. Generic Query Execution Function ---
def execute_query(query, params=None, conn_reuse=False):
    """
    Executes a given SQL query with optional parameters.
    Returns a Pandas DataFrame for SELECT queries, None for others.
    Args:
        query (str): The SQL query string.
        params (tuple, optional): A tuple of parameters to substitute into the query.
        conn_reuse (bool): If True, assumes an existing connection is passed and doesn't close it.
                           Used internally for setup_database to avoid re-connecting multiple times.
    Returns:
        pd.DataFrame or None: DataFrame for SELECT queries, None for INSERT/UPDATE/DELETE.
    """
    conn = None
    cur = None
    try:
        if not conn_reuse: # Only connect if not reusing an existing connection
            conn = connect_db()
            if conn is None:
                return pd.DataFrame() if query.strip().upper().startswith('SELECT') else None
        else: # If reusing, assume connect_db was called externally and connection is passed
            conn = connect_db() # Re-connect for simplicity here, but in a real scenario, pass conn

        cur = conn.cursor()
        cur.execute(query, params)

        if query.strip().upper().startswith('SELECT'):
            columns = [desc[0] for desc in cur.description]
            records = cur.fetchall()
            df = pd.DataFrame(records, columns=columns)
            return df
        else:
            conn.commit() # Commit changes for INSERT, UPDATE, DELETE
            return None # No DataFrame for non-SELECT queries
    except psycopg2.Error as e:
        if conn and not conn_reuse: # Only rollback if this function initiated the connection
            conn.rollback()
        print(f"Error executing query: '{query}' with params '{params}': {e}")
        return pd.DataFrame() if query.strip().upper().startswith('SELECT') else None # Return empty DF on error for SELECTs
    except Exception as e:
        print(f"An unexpected error occurred while executing query: {e}")
        return pd.DataFrame() if query.strip().upper().startswith('SELECT') else None
    finally:
        if cur:
            cur.close()
        if conn and not conn_reuse: # Only close if this function initiated the connection
            conn.close()

# --- 7. CRUD Operations (Specific Functions) ---

# Add Provider
def add_provider(name, type, address, city, contact):
    """Adds a new provider record to the database."""
    query = """
    INSERT INTO providers (name, type, address, city, contact)
    VALUES (%s, %s, %s, %s, %s) RETURNING provider_id;
    """
    conn = connect_db()
    if conn is None: return None
    try:
        cur = conn.cursor()
        cur.execute(query, (name, type, address, city, contact))
        provider_id = cur.fetchone()[0] # Get the ID of the newly inserted provider
        conn.commit()
        print(f"Provider '{name}' added with ID: {provider_id}")
        return provider_id
    except psycopg2.IntegrityError as e:
        conn.rollback()
        print(f"Error adding provider (IntegrityError): {e}")
        return None
    except Exception as e:
        conn.rollback()
        print(f"Error adding provider: {e}")
        return None
    finally:
        if cur: cur.close()
        if conn: conn.close()

# Update Claim Status
def update_claim_status(claim_id, new_status):
    """Updates the status of a specific claim."""
    query = """
    UPDATE claims
    SET status = %s
    WHERE claim_id = %s;
    """
    conn = connect_db()
    if conn is None: return False
    try:
        cur = conn.cursor()
        cur.execute(query, (new_status, claim_id))
        conn.commit()
        if cur.rowcount > 0:
            print(f"Claim {claim_id} status updated to '{new_status}'.")
            return True
        else:
            print(f"Claim {claim_id} not found.")
            return False
    except Exception as e:
        conn.rollback()
        print(f"Error updating claim status: {e}")
        return False
    finally:
        if cur: cur.close()
        if conn: conn.close()

# Delete Food Listing
def delete_food_listing(food_id):
    """
    Deletes a food listing and cascades to delete associated claims.
    (Due to ON DELETE CASCADE defined in table schema)
    """
    query = "DELETE FROM food WHERE food_id = %s;"
    conn = connect_db()
    if conn is None: return False
    try:
        cur = conn.cursor()
        cur.execute(query, (food_id,))
        conn.commit()
        if cur.rowcount > 0:
            print(f"Food listing {food_id} and associated claims deleted.")
            return True
        else:
            print(f"Food listing {food_id} not found.")
            return False
    except Exception as e:
        conn.rollback()
        print(f"Error deleting food listing: {e}")
        return False
    finally:
        if cur: cur.close()
        if conn: conn.close()

# Get All Food Listings (Example of a specific GET function)
def get_all_food_listings():
    """Fetches all records from the food_listings table."""
    query = "SELECT * FROM food ORDER BY food_id DESC;"
    return execute_query(query)

# You can add similar specific CRUD functions for Receivers and Claims as needed.
# For example:
# def add_receiver(name, type, city, contact): ...
# def add_claim(food_id, receiver_id, status): ...
# def delete_receiver(receiver_id): ...
# def delete_claim(claim_id): ...