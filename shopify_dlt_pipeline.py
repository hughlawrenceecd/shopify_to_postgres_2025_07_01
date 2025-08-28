import logging
import dlt
from dlt.common import pendulum
from typing import List, Tuple
from shopify_dlt import shopify_source, TAnyDateTime, shopify_partner_query

# ---- Setup logging ----
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for very verbose
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True  # overwrite default config if DLT sets one
)
logger = logging.getLogger(__name__)


def load_all_resources(resources: List[str], start_date: TAnyDateTime) -> None:
    logger.info(f"📦 Resources: {resources}")
    logger.info(f"⏳ Start date: {start_date}")

    pipeline = dlt.pipeline(
        pipeline_name="shopify_july", destination="postgres", dataset_name="shopify_data"
    )

    try:
        logger.info("🔌 Initializing pipeline...")
        source = shopify_source(start_date=start_date).with_resources(*resources)
        logger.info("⚙️ Source configured, running pipeline...")

        load_info = pipeline.run(source)

        logger.info("✅ Pipeline run complete.")
        logger.info(f"📊 Load info summary:\n{load_info}")
    except Exception as e:
        logger.exception("❌ Pipeline failed with error")
        raise


def incremental_load_with_backloading() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shopify", destination="postgres", dataset_name="shopify_data"
    )

    min_start_date = current_start_date = pendulum.datetime(2025, 7, 1)
    max_end_date = pendulum.now()

    ranges: List[Tuple[pendulum.DateTime, pendulum.DateTime]] = []
    while current_start_date < max_end_date:
        end_date = min(current_start_date.add(weeks=1), max_end_date)
        ranges.append((current_start_date, end_date))
        current_start_date = end_date

    logger.info(f"🔄 Starting backfill with {len(ranges)} weekly chunks")

    for idx, (start_date, end_date) in enumerate(ranges, start=1):
        logger.info(f"▶️ Chunk {idx}/{len(ranges)}: {start_date} → {end_date}")
        data = shopify_source(
            start_date=start_date, end_date=end_date, created_at_min=min_start_date
        ).with_resources("orders")

        try:
            load_info = pipeline.run(data)
            logger.info(f"✅ Chunk {idx} completed: {load_info}")
        except Exception as e:
            logger.exception(f"❌ Failed on chunk {idx} ({start_date} → {end_date})")
            break

    logger.info("🔄 Switching to incremental load from latest backfill point...")
    load_info = pipeline.run(
        shopify_source(
            start_date=max_end_date, created_at_min=min_start_date
        ).with_resources("orders")
    )
    logger.info(f"✅ Incremental load complete: {load_info}")


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


if __name__ == "__main__":
    # Add your desired resources to the list...
    # resources = ["products", "orders", "customers"]
    # load_all_resources(resources, start_date="2025-07-01")

    incremental_load_with_backloading()

    # load_partner_api_transactions()