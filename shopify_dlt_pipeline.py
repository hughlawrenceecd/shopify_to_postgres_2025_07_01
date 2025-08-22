"""Pipeline to load Shopify data into DuckDB with GDPR redaction support."""

import dlt
from dlt.common import pendulum
from typing import List, Tuple, Optional
from shopify_dlt import shopify_source, TAnyDateTime, shopify_partner_query
import duckdb
import hashlib
from pathlib import Path

# Add these constants at the top
DB_PATH = Path("/Users/hugh.lawrence/Documents/dlt_shopify_test/shopify.duckdb")
SCHEMA = "shopify_data"

PII_COLUMN_ACTIONS = {
    "email": "hash",
    "contact_email": "hash",
    "first_name": "anon",
    "customer__first_name": "anon",
    "customer__last_name": "anon",
    "customer__email": "hash",
    "customer__phone": "null",
    "customer__default_address__first_name": "anon",
    "customer__default_address__last_name": "anon",
    "customer__default_address__address1": "null",
    "customer__default_address__city": "null",
    "customer__default_address__zip": "null",
    "customer__default_address__phone": "null",
    "last_name": "anon",
    "name": "anon",
    "phone": "null",
    "address1": "null",
    "address2": "null",
    "default_address__first_name": "null",
    "default_address__last_name": "null",
    "default_address__address1": "null",
    "default_address__city": "null",
    "default_address__province": "null",
    "default_address__country": "null",
    "default_address__zip": "null",
    "default_address__phone": "null",
    "default_address__name": "null",
    "default_address__country_code": "null",
    "default_address__country_name": "null",
    "default_address__default": "null",
    "shipping_address__address1": "null",
    "shipping_address__phone": "null",
    "shipping_address__zip": "null",
    "city": "null",
    "province": "null",
    "province_code": "null",
    "zip": "null",
    "postal_code": "null",
    "country": "null",
    "country_code": "null",
    "company": "null",
    "company_name": "null",
    "notes": "null",
    "tags": "null",
}

ANON_FIRST = "Anonymous"
ANON_LAST = "Customer"

def _hash_value(value: Optional[str], salt: Optional[str] = None) -> str:
    if not value:
        value = ""
    m = hashlib.sha256()
    if salt:
        m.update(salt.encode())
    m.update(value.encode())
    return m.hexdigest()

def anonymize_customer(customer_id: int, db_path: Optional[Path] = None, schema: str = SCHEMA, email_salt: Optional[str] = None):
    db_path = Path(db_path) if db_path else DB_PATH
    con = duckdb.connect(str(db_path))
    
    # Create redacted customers tracking table
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.redacted_customers (
            customer_id BIGINT PRIMARY KEY,
            redacted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Mark this customer as redacted
    con.execute(f"""
        INSERT OR IGNORE INTO {schema}.redacted_customers (customer_id) 
        VALUES (?)
    """, (customer_id,))
    
    tables_to_update = {
        "customers": "id",
        "customers__addresses": "customer_id",
        "orders": "customer__id",
        "orders": "customer__default_address__customer_id"
    }

    salt = email_salt

    for table, id_col in tables_to_update.items():
        # Get columns for this table
        cols_query = f"PRAGMA table_info('{schema}.{table}')"
        cols_info = con.execute(cols_query).fetchall()
        cols = {col[1].lower(): col[1] for col in cols_info}  # map lowercase -> actual column name
        
        set_clauses = []
        values = []

        # For hashing email, fetch current value first
        email_col = cols.get("email")
        hashed_email = None
        if email_col and PII_COLUMN_ACTIONS.get("email") == "hash":
            row = con.execute(f"SELECT {email_col} FROM {schema}.{table} WHERE {id_col} = ? LIMIT 1", (customer_id,)).fetchone()
            current_email = row[0] if row else None
            hashed_email = _hash_value(current_email, salt)

        for pii_col, action in PII_COLUMN_ACTIONS.items():
            if pii_col in cols:
                col_name = cols[pii_col]
                if action == "hash" and pii_col == "email":
                    set_clauses.append(f"{col_name} = ?")
                    values.append(hashed_email)
                elif action == "hash":
                    # fallback: nullify
                    set_clauses.append(f"{col_name} = NULL")
                elif action == "null":
                    set_clauses.append(f"{col_name} = NULL")
                elif action == "anon":
                    if pii_col == "first_name":
                        set_clauses.append(f"{col_name} = ?")
                        values.append(ANON_FIRST)
                    elif pii_col == "last_name":
                        set_clauses.append(f"{col_name} = ?")
                        values.append(ANON_LAST)
                    else:
                        set_clauses.append(f"{col_name} = ?")
                        values.append("REDACTED")

        if not set_clauses:
            continue  # nothing to update in this table

        set_sql = ", ".join(set_clauses)
        sql = f"UPDATE {schema}.{table} SET {set_sql} WHERE {id_col} = ?"
        values.append(customer_id)

        try:
            con.execute(sql, tuple(values))
            print(f"[✅] Anonymized customer {customer_id} in table '{table}'.")
        except Exception as e:
            print(f"[ERROR] Failed to update table '{table}': {e}")

    con.close()

def reapply_redactions(db_path: Optional[Path] = None, schema: str = SCHEMA):
    """Re-apply anonymization to all previously redacted customers"""
    db_path = Path(db_path) if db_path else DB_PATH
    con = duckdb.connect(str(db_path))
    
    # Check if redacted_customers table exists
    table_exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = '{schema}' AND table_name = 'redacted_customers'
    """).fetchone()[0]
    
    if not table_exists:
        print("No redacted customers table found - skipping reapplication")
        con.close()
        return
    
    # Get all redacted customer IDs
    redacted_customers = con.execute(
        f"SELECT customer_id FROM {schema}.redacted_customers"
    ).fetchall()
    
    con.close()
    
    # Re-apply anonymization to each customer
    for (customer_id,) in redacted_customers:
        print(f"Re-applying anonymization to customer {customer_id}")
        anonymize_customer(customer_id, db_path, schema)

def load_all_resources(resources: List[str], start_date: TAnyDateTime) -> None:
    """Execute a pipeline that will load the given Shopify resources incrementally beginning at the given start date.
    Subsequent runs will load only items updated since the previous run.
    """

    pipeline = dlt.pipeline(
        pipeline_name="shopify", destination='duckdb', dataset_name="shopify_data"
    )
    
    # Load data from Shopify
    load_info = pipeline.run(
        shopify_source(start_date=start_date).with_resources(*resources),
    )
    
    # Re-apply redactions to ensure compliance
    reapply_redactions()
    
    print(load_info)

def incremental_load_with_backloading() -> None:
    """Load past orders from Shopify in chunks of 1 week each."""

    pipeline = dlt.pipeline(
        pipeline_name="shopify", destination='postgres', dataset_name="shopify_data"
    )

    min_start_date = current_start_date = pendulum.datetime(2024, 1, 1)
    max_end_date = pendulum.now()

    ranges: List[Tuple[pendulum.DateTime, pendulum.DateTime]] = []
    while current_start_date < max_end_date:
        end_date = min(current_start_date.add(weeks=1), max_end_date)
        ranges.append((current_start_date, end_date))
        current_start_date = end_date

    with pipeline:  # ✅ KEEP CONNECTION OPEN DURING ALL RUNS
        for start_date, end_date in ranges:
            print(f"Load orders between {start_date} and {end_date}")
            data = shopify_source(
                start_date=start_date, end_date=end_date, created_at_min=min_start_date
            ).with_resources("orders")

            load_info = pipeline.run(data)
            print(load_info)

        # Final incremental run starting from now
        load_info = pipeline.run(
            shopify_source(
                start_date=max_end_date, created_at_min=min_start_date
            ).with_resources("orders")
        )
        print(load_info)
        
        # Re-apply redactions after all loading is complete
        reapply_redactions()

def load_partner_api_transactions() -> None:
    """Load transactions from the Shopify Partner API.
    The partner API uses GraphQL and this example loads all transactions from the beginning paginated.

    The `shopify_partner_query` resource can be used to run custom GraphQL queries to load paginated data.
    """

    pipeline = dlt.pipeline(
        pipeline_name="shopify_partner",
        destination='postgres',
        dataset_name="shopify_partner_data",
    )

    # Construct query to load transactions 100 per page, the `$after` variable is used to paginate
    query = """query Transactions($after: String, first: 100) {
        transactions(after: $after) {
            edges {
                cursor
                node {
                    id
                }
            }
        }
    }
    """

    # Configure the resource with the query and json paths to extract the data and pagination cursor
    resource = shopify_partner_query(
        query,
        # JSON path pointing to the data item in the results
        data_items_path="data.transactions.edges[*].node",
        # JSON path pointing to the highest page cursor in the results
        pagination_cursor_path="data.transactions.edges[-1].cursor",
        # The variable name used for pagination
        pagination_variable_name="after",
    )

    load_info = pipeline.run(resource)
    print(load_info)

# Add this function for webhook handling
def handle_redaction_webhook(customer_id: int):
    """Call this function when you receive a customers/redacted webhook"""
    print(f"Received redaction request for customer {customer_id}")
    anonymize_customer(customer_id)
    print(f"Customer {customer_id} has been anonymized")

if __name__ == "__main__":
    # Add your desired resources to the list...
    resources = ["products", "orders", "customers"]
    load_all_resources(resources, start_date="2025-07-01")

    # incremental_load_with_backloading()
    # load_partner_api_transactions()
    
    # Example of handling a webhook (you'd call this from your webhook endpoint)
    # handle_redaction_webhook(123456789)  # Replace with actual customer ID