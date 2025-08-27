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
        if field in anonymized and anonymized[field] is not None:
            if action == "hash":
                anonymized[field] = _hash_value(str(anonymized[field]), salt)
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
    # Get the standard customers resource
    source = shopify_source(start_date=start_date)
    customers_gen = source.with_resources("customers").resources["customers"]()
    
    for customer in customers_gen:
        customer_id = customer.get("id")
        if customer_id and customer_id in redacted_customers:
            yield anonymize_customer_data(customer)
        else:
            yield customer

@dlt.resource(name="orders", write_disposition="merge")
def orders_with_redaction(start_date: TAnyDateTime, redacted_customers: set) -> Iterator[Dict[str, Any]]:
    """Order resource that applies redaction for orders from redacted customers."""
    # Get the standard orders resource
    source = shopify_source(start_date=start_date)
    orders_gen = source.with_resources("orders").resources["orders"]()
    
    for order in orders_gen:
        customer_id = None
        customer_data = order.get("customer")
        
        if isinstance(customer_data, dict):
            customer_id = customer_data.get("id")
        elif customer_data is not None:
            # Handle case where customer might be a simple value
            customer_id = customer_data
        
        if customer_id and customer_id in redacted_customers:
            # Anonymize customer data within the order
            if "customer" in order and isinstance(order["customer"], dict):
                order["customer"] = anonymize_customer_data(order["customer"])
            
            # Also check for customer data in nested fields
            for key in list(order.keys()):
                if key.startswith("customer__") and order[key] is not None:
                    # For flat fields, apply individual anonymization
                    field_name = key.replace("customer__", "", 1)
                    if field_name in PII_COLUMN_ACTIONS:
                        action = PII_COLUMN_ACTIONS[field_name]
                        if action == "hash":
                            order[key] = _hash_value(str(order[key]))
                        elif action == "null":
                            order[key] = None
                        elif action == "anon":
                            if field_name == "first_name":
                                order[key] = ANON_FIRST
                            elif field_name == "last_name":
                                order[key] = ANON_LAST
                            else:
                                order[key] = "REDACTED"
            
            yield order
        else:
            yield order

@dlt.resource(name="products", write_disposition="merge")
def products_resource(start_date: TAnyDateTime) -> Iterator[Dict[str, Any]]:
    """Standard products resource."""
    source = shopify_source(start_date=start_date)
    products_gen = source.with_resources("products").resources["products"]()
    yield from products_gen

def mark_customer_redacted(customer_id: int):
    """Mark a customer as redacted (in production, persist this to a database)."""
    REDACTED_CUSTOMERS.add(customer_id)
    print(f"Marked customer {customer_id} for redaction")

def load_all_resources(resources: List[str], start_date: TAnyDateTime) -> None:
    """Execute a pipeline that will load the given Shopify resources with redaction support."""
    
    pipeline = dlt.pipeline(
        pipeline_name="shopify", destination='postgres', dataset_name="shopify_data"
    )
    
    # Create the appropriate resources based on what's requested
    transformed_resources = []
    for resource_name in resources:
        if resource_name == "customers":
            transformed_resources.append(customers_with_redaction(start_date, REDACTED_CUSTOMERS))
        elif resource_name == "orders":
            transformed_resources.append(orders_with_redaction(start_date, REDACTED_CUSTOMERS))
        elif resource_name == "products":
            transformed_resources.append(products_resource(start_date))
        else:
            # For other resources, create them dynamically
            source = shopify_source(start_date=start_date)
            resource = source.with_resources(resource_name)
            transformed_resources.append(resource)
    
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
        
        # Create source with date range
        source = shopify_source(start_date=start_date, end_date=end_date)
        orders_resource = source.with_resources("orders")
        
        load_info = pipeline.run(orders_resource)
        print(load_info)

    # Final incremental run with redaction support
    load_info = pipeline.run(
        orders_with_redaction(
            start_date=max_end_date, 
            redacted_customers=REDACTED_CUSTOMERS
        )
    )
    print(load_info)

def load_partner_api_transactions() -> None:
    """Load transactions from the Shopify Partner API."""
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
    load_all_resources(resources, start_date="2025-06-06")

    # incremental_load_with_backloading()
    # load_partner_api_transactions()
    
    # Example webhook handling
    # handle_redaction_webhook(123456789)