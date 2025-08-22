"""Pipeline to load Shopify data with GDPR-compliant anonymization."""

import dlt
from dlt.common import pendulum
from typing import List, Tuple, Optional, Iterator, Dict, Any
from shopify_dlt import shopify_source, TAnyDateTime, shopify_partner_query
import hashlib

# Constants for anonymization
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

# Global set to track redacted customers (in production, use Redis or database)
REDACTED_CUSTOMERS = set()

def _hash_value(value: Optional[str], salt: Optional[str] = None) -> str:
    """Hash a value using SHA256."""
    if not value:
        return ""
    m = hashlib.sha256()
    if salt:
        m.update(salt.encode())
    m.update(value.encode())
    return m.hexdigest()

def anonymize_customer_data(customer_data: Dict[str, Any], salt: Optional[str] = None) -> Dict[str, Any]:
    """Apply anonymization to a customer data dictionary."""
    anonymized = customer_data.copy()
    
    for field, action in PII_COLUMN_ACTIONS.items():
        if field in anonymized:
            if action == "hash":
                anonymized[field] = _hash_value(anonymized.get(field), salt)
            elif action == "null":
                anonymized[field] = None
            elif action == "anon":
                if field == "first_name" or field.endswith("_first_name"):
                    anonymized[field] = ANON_FIRST
                elif field == "last_name" or field.endswith("_last_name"):
                    anonymized[field] = ANON_LAST
                else:
                    anonymized[field] = "REDACTED"
    
    return anonymized

@dlt.resource(name="customers", write_disposition="merge")
def customers_with_redaction(start_date: TAnyDateTime, redacted_customers: set) -> Iterator[Dict[str, Any]]:
    """Customer resource that applies redaction to known redacted customers."""
    customers_resource = shopify_source(start_date=start_date).with_resources("customers")["customers"]
    
    for customer in customers_resource:
        customer_id = customer.get("id")
        if customer_id and customer_id in redacted_customers:
            yield anonymize_customer_data(customer)
        else:
            yield customer

@dlt.resource(name="orders", write_disposition="merge")
def orders_with_redaction(start_date: TAnyDateTime, redacted_customers: set) -> Iterator[Dict[str, Any]]:
    """Order resource that applies redaction for orders from redacted customers."""
    orders_resource = shopify_source(start_date=start_date).with_resources("orders")["orders"]
    
    for order in orders_resource:
        customer_id = order.get("customer", {}).get("id") if isinstance(order.get("customer"), dict) else None
        if customer_id and customer_id in redacted_customers:
            # Anonymize customer data within the order
            if "customer" in order and isinstance(order["customer"], dict):
                order["customer"] = anonymize_customer_data(order["customer"])
            
            # Also check for customer data in nested fields
            for key in list(order.keys()):
                if key.startswith("customer__") and order[key] is not None:
                    # Create a mock customer dict for anonymization
                    mock_customer = {key.replace("customer__", "", 1): order[key]}
                    anonymized = anonymize_customer_data(mock_customer)
                    order[key] = anonymized.get(key.replace("customer__", "", 1))
            
            yield order
        else:
            yield order

def mark_customer_redacted(customer_id: int):
    """Mark a customer as redacted (in production, persist this to a database)."""
    REDACTED_CUSTOMERS.add(customer_id)
    print(f"Marked customer {customer_id} for redaction")

def load_all_resources(resources: List[str], start_date: TAnyDateTime) -> None:
    """Execute a pipeline that will load the given Shopify resources with redaction support."""
    
    pipeline = dlt.pipeline(
        pipeline_name="shopify", destination='postgres', dataset_name="shopify_data"
    )
    
    # Create the source with redaction-aware resources
    source = shopify_source(start_date=start_date)
    
    # Replace standard resources with redaction-aware versions if they exist
    transformed_resources = []
    for resource_name in resources:
        if resource_name == "customers":
            transformed_resources.append(customers_with_redaction(start_date, REDACTED_CUSTOMERS))
        elif resource_name == "orders":
            transformed_resources.append(orders_with_redaction(start_date, REDACTED_CUSTOMERS))
        else:
            # For other resources (like products), use the standard version
            transformed_resources.append(source.with_resources(resource_name)[resource_name])
    
    load_info = pipeline.run(transformed_resources)
    print(load_info)

def incremental_load_with_backloading() -> None:
    """Load past orders from Shopify in chunks of 1 week each with redaction support."""
    
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

    # Run the pipeline for each time range
    for start_date, end_date in ranges:
        print(f"Load orders between {start_date} and {end_date}")
        
        # Use redaction-aware orders resource
        data = orders_with_redaction(
            start_date=start_date, 
            redacted_customers=REDACTED_CUSTOMERS
        )
        
        load_info = pipeline.run(data)
        print(load_info)

    # Final incremental run
    load_info = pipeline.run(
        orders_with_redaction(
            start_date=max_end_date, 
            redacted_customers=REDACTED_CUSTOMERS
        )
    )
    print(load_info)

def load_partner_api_transactions() -> None:
    """Load transactions from the Shopify Partner API."""
    # (Unchanged from your original code)
    pipeline = dlt.pipeline(
        pipeline_name="shopify_partner",
        destination='postgres',
        dataset_name="shopify_partner_data",
    )

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

    resource = shopify_partner_query(
        query,
        data_items_path="data.transactions.edges[*].node",
        pagination_cursor_path="data.transactions.edges[-1].cursor",
        pagination_variable_name="after",
    )

    load_info = pipeline.run(resource)
    print(load_info)

# Webhook handler function
def handle_redaction_webhook(customer_id: int):
    """Call this function when you receive a customers/redacted webhook"""
    print(f"Received redaction request for customer {customer_id}")
    mark_customer_redacted(customer_id)
    print(f"Customer {customer_id} has been marked for redaction")

if __name__ == "__main__":
    # Add your desired resources to the list...
    resources = ["products", "orders", "customers"]
    load_all_resources(resources, start_date="2025-01-01")

    # incremental_load_with_backloading()
    # load_partner_api_transactions()
    
    # Example webhook handling
    # handle_redaction_webhook(123456789)