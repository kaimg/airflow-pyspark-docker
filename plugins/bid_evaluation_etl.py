import io
import itertools
from collections import defaultdict

import numpy as np #type: ignore
import pandas as pd #type: ignore
import polars as pl #type: ignore
from loguru import logger #type: ignore
from sqlalchemy import ( #type: ignore
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Identity,
    Numeric,
    String,
    Table,
    insert,
    select,
)


# Normalization tables
def pre_process_data_normalization(df: pd.DataFrame):
    """Pre-process data for normalization tables.
    
    Args:
        df: Input DataFrame containing bid evaluation data
        
    Returns:
        StringIO object containing processed CSV data
    """
    logger.info("Starting data pre-processing for normalization")
    logger.debug(f"Input DataFrame shape: {df.shape}")
    
    # 1. Predefine the `result_data` list
    result_data = []

    # 2. Execute the main code
    unique_suppliers_all = df["Suppliers"].unique()
    min_comb_size = 1
    max_comb_size = len(unique_suppliers_all)
    logger.info(f"Found {len(unique_suppliers_all)} unique suppliers")
    logger.debug(f"Combination size range: {min_comb_size} to {max_comb_size}")

    # Precompute data structures for optimization
    item_supplier_min_valid_row = {}
    item_rows_order = defaultdict(list)
    supplier_items = defaultdict(set)

    # Populate preprocessing data
    logger.info("Populating preprocessing data structures")
    for idx, row in df.iterrows():
        supplier = row["Suppliers"]
        item = row["Item number"]
        supplier_items[supplier].add(item)
        item_rows_order[item].append((supplier, idx))

    logger.info("Processing supplier-item combinations")
    for (item, supplier), group in df.groupby(["Item number", "Suppliers"]):
        valid_prices = group[group["Items price"] > 0]
        if not valid_prices.empty:
            min_idx = valid_prices["Items price"].idxmin()
            item_supplier_min_valid_row[(item, supplier)] = df.loc[min_idx].copy()

    # Process combinations
    logger.info("Processing supplier combinations")
    for comb_size in range(min_comb_size, max_comb_size + 1):
        logger.debug(f"Processing combinations of size {comb_size}")
        for comb in itertools.combinations(unique_suppliers_all, comb_size):
            items_in_comb = set().union(*(supplier_items[s] for s in comb))
            
            total_comb_cost = 0
            total_comb_cost_with_qty = 0
            item_details = []

            for item in items_in_comb:
                valid_prices = []
                for supplier in comb:
                    key = (item, supplier)
                    if key in item_supplier_min_valid_row:
                        valid_prices.append(item_supplier_min_valid_row[key])

                if valid_prices:
                    min_row = min(valid_prices, key=lambda x: x["Items price"])
                    total_comb_cost += min_row["Items price"]
                    total_comb_cost_with_qty += min_row["Items price"] * min_row["QTY"]
                    item_details.append(min_row.copy())
                else:
                    for supplier, idx in item_rows_order[item]:
                        if supplier in comb:
                            first_row = df.loc[idx].copy()
                            first_row["Items price"] = 0
                            item_details.append(first_row)
                            break

            result_data.append({
                "Combination type": "Include off-spec",
                "Combination size": comb_size,
                "Selected suppliers": " and ".join(comb),
                "Total combination cost": total_comb_cost,
                "Total combination cost with QTY": total_comb_cost_with_qty,
                "Item-level details": [row.to_dict() for row in item_details],
            })

    logger.info("Creating final DataFrame")
    df = pd.DataFrame(result_data)
    df = df.rename(columns={"Item-level details": "Detail"})

    def main(df: pd.DataFrame):
        new_rows = []
        keys = []
        for index, row in df.iterrows():
            list_of_details: list[dict] = row["Detail"]

            for key in list_of_details[0].keys():
                if key not in keys:
                    keys.append(key)

            for i in list_of_details:
                row_list = list(row)
                row_list += list(i.values())
                new_rows.append(row_list)

        return pd.DataFrame(new_rows, columns=list(df.columns) + keys)

    sample_df = main(df)
    result = sample_df.drop(columns=["Detail"])
    # result= result.replace(0,np.nan)

    # Calculate median prices and weights without merging
    median_prices = result.groupby("Item number")["Items price"].median()
    total_median_price = median_prices.sum()
    result["Weight"] = result["Item number"].map(median_prices) / total_median_price

    # Calculate supplier weight sums using a mask instead of filtered dataframe
    mask = result["Items price"] != 0
    supplier_weight_sum = result[mask].groupby("Selected suppliers")["Weight"].sum()
    result["Supplier Weight Sum"] = result["Selected suppliers"].map(
        supplier_weight_sum
    )

    # Vectorized flag calculation
    result["Item Price Flag"] = (result["Items price"] != 0).astype(int)
    result = pd.DataFrame(result)

    # Create a dictionary to store the new column values
    coverage_ratios = {}
    for size in range(1, result["Combination size"].max() + 1):
        # Filter out rows with the current combination size and non-zero item price
        df_filtered = result[
            (result["Combination size"] == size) & (result["Items price"] != 0)
        ]
        # Group by supplier and count the items
        supplier_item_counts = df_filtered.groupby("Selected suppliers")[
            "Item number"
        ].count()
        # Get the total number of items per supplier for the current combination size
        total_items_per_supplier = (
            result[result["Combination size"] == size]
            .groupby("Selected suppliers")["Item number"]
            .count()
        )
        # Calculate the ratio
        ratio = supplier_item_counts / total_items_per_supplier
        # Store the results in the dictionary
        coverage_ratios.update(ratio.to_dict())
    # Add the new column to the main dataframe
    result["Item Coverage Ratio"] = result["Selected suppliers"].map(coverage_ratios)

    # Columns that should be excluded from replacement
    exclude_columns = [
        "Median price",
        "Weight",
        "Supplier Weight Sum",
        "Item Price Flag",
        "Jaccard Similarity",
        "Item number",
    ]
    # Replace zeros with NaN except for the specified columns
    result = result.apply(
        lambda x: x.replace(0, np.nan) if x.name not in exclude_columns else x
    )

    for size in range(1, result["Combination size"].max() + 1):
        # Filter data where "Combination size" is the current size and "Items price" is not null
        df_filtered = result[
            (result["Combination size"] == size) & (result["Items price"].notnull())
        ]
        # Find intersecting item numbers across different suppliers
        supplier_item_sets = df_filtered.groupby("Selected suppliers")[
            "Item number"
        ].apply(set)
        # Find the intersection of item numbers across all suppliers in the dataset
        common_items = set.intersection(
            *supplier_item_sets
        )  # if len(supplier_item_sets) > 1 else set()
        # Count the number of intersecting item numbers
        common_item_count = len(common_items)
        # Assign the common item count to the corresponding rows
        result.loc[result["Combination size"] == size, "Common Item Count"] = (
            common_item_count
        )
        result.loc[result["Combination size"] == size, "Common Items"] = result[
            "Item number"
        ].apply(lambda x: "+" if x in common_items else "~")

    # Filter only rows where "Common Items" is "+"
    df_common = result[result["Common Items"] == "+"]

    # For each supplier, find the minimum prices among the items they offer
    supplier_min_prices = (
        df_common.groupby(["Selected suppliers", "Item number"])["Items price"]
        .min()
        .reset_index()
    )
    supplier_min_prices.rename(columns={"Items price": "Min Items price"}, inplace=True)

    # Keep only the rows that match these minimum prices
    df_min_cost = df_common.merge(
        supplier_min_prices,
        left_on=["Selected suppliers", "Item number", "Items price"],
        right_on=["Selected suppliers", "Item number", "Min Items price"],
    )

    # For each supplier, calculate the total of these minimum prices
    common_price_sum = (
        df_min_cost.groupby("Selected suppliers")["Items price"].sum().reset_index()
    )
    common_price_sum.rename(
        columns={"Items price": "Common Item Total Price"}, inplace=True
    )

    # Merge the result back into the main dataframe
    result = result.merge(common_price_sum, on="Selected suppliers", how="left")
    result.drop(["Combination type", "Item Price Flag"], axis=1, inplace=True)

    # Write the output to an IO object
    output = io.StringIO()
    result.to_csv(output, index=False)
    output.seek(0)
    
    logger.info("Pre-processing completed successfully")
    return output


def create_tables_normalization(
    table_name_prefix, metadata, engine, create_tables=True
):
    """Create database tables for bid evaluation data."""

    # --- Table Definitions (SQLAlchemy Core) ---
    suppliers_table = Table(
        f"{table_name_prefix}suppliers",
        metadata,
        Column(
            "id",
            BigInteger,
            Identity(),
            primary_key=True,
            autoincrement=True,
            comment="Unique identifier for the supplier.",
        ),
        Column("name", String, comment="Name of the supplier."),
        comment="Stores information about suppliers.",
        extend_existing=True
    )

    items_table = Table(
        f"{table_name_prefix}items",
        metadata,
        Column(
            "id",
            BigInteger,
            Identity(),
            primary_key=True,
            autoincrement=True,
            comment="Unique identifier for the item.",
        ),
        Column("quantity", BigInteger, comment="The required quantity of the item."),
        Column(
            "item_number",
            String,
            comment="Unique identifier for the item (can be string).",
        ),
        comment="Stores information about the items being bid on.",
        extend_existing=True
    )

    item_offers_table = Table(
        f"{table_name_prefix}item_offers",
        metadata,
        Column(
            "id",
            BigInteger,
            Identity(),
            primary_key=True,
            autoincrement=True,
            comment="Unique identifier for the item offer.",
        ),
        Column(
            "item_id",
            BigInteger,
            ForeignKey(f"{table_name_prefix}items.id"),
            comment="Foreign key referencing the items table.",
        ),
        Column(
            "supplier_id",
            BigInteger,
            ForeignKey(f"{table_name_prefix}suppliers.id"),
            comment="Foreign key referencing the suppliers table.",
        ),
        Column(
            "price",
            Numeric,
            comment="The price offered by the supplier for the item (in the offer's currency).",
        ),
        Column(
            "currency", String, comment="The currency in which the price is offered."
        ),
        Column("uom", String, comment="Unit of Measure for the item in this offer."),
        comment="Stores individual offers from suppliers for specific items.",
        extend_existing=True
    )

    combinations_table = Table(
        f"{table_name_prefix}combinations",
        metadata,
        Column(
            "id",
            BigInteger,
            Identity(),
            primary_key=True,
            autoincrement=True,
            comment="Unique identifier for the supplier combination.",
        ),
        Column(
            "suppliers",
            ARRAY(BigInteger),
            comment="Array of supplier IDs included in this combination.",
        ),
        Column(
            "supplier_names",
            String,
            comment="Comma-separated list of supplier names in this combination.",
        ),
        Column(
            "total_cost_unit",
            Numeric,
            comment="Total cost of the combination, based on the best unit price for each item.",
        ),
        Column(
            "total_cost_qty",
            Numeric,
            comment="Total cost of the combination, considering the required quantity of each item.",
        ),
        Column(
            "coverage",
            Numeric,
            comment="The ratio of items covered by this combination to the total number of items.",
        ),
        comment="Represents different combinations of suppliers.",
        extend_existing=True
    )

    combination_item_offers_table = Table(
        f"{table_name_prefix}combination_item_offers",
        metadata,
        Column(
            "id",
            BigInteger,
            Identity(),
            primary_key=True,
            autoincrement=True,
            comment="Unique identifier for this record.",
        ),
        Column(
            "item_id",
            BigInteger,
            ForeignKey(f"{table_name_prefix}items.id"),
            comment="Foreign key referencing the items table.",
        ),
        Column(
            "combination_id",
            BigInteger,
            ForeignKey(f"{table_name_prefix}combinations.id"),
            comment="Foreign key referencing the combinations table.",
        ),
        Column(
            "weight",
            Numeric,
            comment="The calculated weight of the item (relative price importance).",
        ),
        Column(
            "is_common_item",
            Boolean,
            comment="Indicates if the item has offers from all suppliers in the combination.",
        ),
        Column(
            "best_supplier",
            BigInteger,
            comment="Foreign key referencing the suppliers table, indicating the supplier with the best offer for this item within this combination.",
        ),
        comment="Connects combinations to the best item offers within that combination.",
        extend_existing=True
    )

    # Create tables
    if create_tables:
        metadata.create_all(engine)

    return {
        "suppliers": suppliers_table,
        "items": items_table,
        "item_offers": item_offers_table,
        "combinations": combinations_table,
        "combination_item_offers": combination_item_offers_table,
    }


def load_data_normalization(csv_file, tables, engine):
    """Process CSV data and fill database tables.

    Args:
        csv_file: File object or path to the CSV file containing bid evaluation data
        tables: Dictionary of SQLAlchemy Table objects
        engine: SQLAlchemy engine instance
    """
    logger.info("Starting data loading for normalization tables")
    
    df = pl.read_csv(csv_file)
    logger.debug(f"Loaded CSV data with shape: {df.shape}")

    # Rename columns
    logger.info("Renaming columns")
    df = df.rename({
        "Selected suppliers": "supplier_combination",
        "Suppliers": "best_supplier",
        "Combination size": "combination_size",
        "Currency": "item_currency",
        "Item number": "item_number",
        "Items price": "item_price",
        "Total combination cost": "total_combination_cost_for_one",
        "Total combination cost with QTY": "total_combination_cost_for_quantity",
        "Weight": "weight",
        "Supplier Weight Sum": "weight_sum_for_combination",
        "Item Coverage Ratio": "item_coverage_ratio",
        "Common Item Count": "common_item_count",
        "Common Items": "is_common_item",
        "Common Item Total Price": "common_item_total_price",
        "UoM": "uom",
        "QTY": "qty",
    }).with_columns(
        pl.col("supplier_combination").str.split(" and ").cast(list[str]),
        pl.col("is_common_item").replace_strict({"+": True, "~": False}),
    )

    # 1. Suppliers
    logger.info("Processing suppliers data")
    supplier_names = sorted(
        (df.select(pl.col("supplier_combination").explode()))["supplier_combination"]
        .unique()
        .to_list()
    )
    supplier_names = [{"name": name} for name in supplier_names]
    logger.debug(f"Found {len(supplier_names)} unique suppliers")

    # Insert suppliers and get their IDs
    logger.info("Inserting suppliers into database")
    stmt = insert(tables["suppliers"]).values(supplier_names)
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

    with engine.connect() as conn:
        supplier_data_from_db = conn.execute(
            select(tables["suppliers"].c.id, tables["suppliers"].c.name)
        ).fetchall()
    supplier_id_map = {name: id for id, name in supplier_data_from_db}
    logger.debug(f"Created supplier ID mapping with {len(supplier_id_map)} entries")

    # 2. Items
    logger.info("Processing items data")
    items_data = (
        df.select(pl.col("item_number"), pl.col("qty").alias("quantity"))
        .unique("item_number")
        .to_dicts()
    )
    logger.debug(f"Found {len(items_data)} unique items")

    logger.info("Inserting items into database")
    stmt = insert(tables["items"]).values(items_data)
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

    with engine.connect() as conn:
        items_data_from_db = conn.execute(
            select(tables["items"].c.id, tables["items"].c.item_number)
        ).fetchall()
    item_id_map = {item_number: id for id, item_number in items_data_from_db}
    logger.debug(f"Created item ID mapping with {len(item_id_map)} entries")

    # Replace references with IDs
    logger.info("Replacing references with IDs")
    df = df.with_columns(
        pl.col("item_number").replace_strict(item_id_map),
        pl.col("best_supplier").replace_strict(supplier_id_map),
        pl.col("supplier_combination")
        .map_elements(
            lambda x: [supplier_id_map[supplier] for supplier in x],
            return_dtype=list[int],
        )
        .alias("supplier_combination"),
    )

    # 3. Item Offers
    logger.info("Processing item offers")
    item_offers_data = (
        df.filter(pl.col("combination_size") == 1)
        .select(
            "item_number",
            "best_supplier",
            "item_price",
            "item_currency",
            "uom",
        )
        .rename({
            "item_number": "item_id",
            "best_supplier": "supplier_id",
            "item_price": "price",
            "item_currency": "currency",
        })
        .to_dicts()
    )
    logger.debug(f"Found {len(item_offers_data)} item offers")

    logger.info("Inserting item offers into database")
    stmt = insert(tables["item_offers"]).values(item_offers_data)
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

    # 4. Combinations
    logger.info("Processing combinations")
    
    # Get supplier names mapping
    with engine.connect() as conn:
        supplier_names = conn.execute(
            select(tables["suppliers"].c.id, tables["suppliers"].c.name)
        ).fetchall()
    supplier_name_map = {id_: name for id_, name in supplier_names}
    
    combinations_df = df.select(
        pl.col("supplier_combination").alias("suppliers"),
        pl.col("total_combination_cost_for_one").alias("total_cost_unit"),
        pl.col("total_combination_cost_for_quantity").alias("total_cost_qty"),
        pl.col("item_coverage_ratio").alias("coverage"),
    ).unique("suppliers")
    
    # Add supplier names column
    combinations_df = combinations_df.with_columns(
        pl.col("suppliers").map_elements(
            lambda x: ",".join(supplier_name_map[supplier_id] for supplier_id in x)
        ).alias("supplier_names")
    )
    
    logger.debug(f"Found {len(combinations_df)} unique combinations")

    logger.info("Inserting combinations into database")
    stmt = insert(tables["combinations"]).values(combinations_df.to_dicts())
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

    with engine.connect() as conn:
        combinations_data_from_db = conn.execute(
            select(tables["combinations"].c.id, tables["combinations"].c.suppliers)
        ).fetchall()

    combination_id_map = {
        tuple(suppliers): id_ for id_, suppliers in combinations_data_from_db
    }
    logger.debug(f"Created combination ID mapping with {len(combination_id_map)} entries")

    # Replace references to combination_id
    logger.info("Replacing combination references with IDs")
    df = df.with_columns(
        pl.col("supplier_combination").map_elements(
            lambda x: combination_id_map[tuple(x)], return_dtype=int
        )
    )

    # 5. Combination Item Offers
    logger.info("Processing combination item offers")
    combination_item_offers_data = df.select(
        pl.col("weight"),
        pl.col("is_common_item"),
        pl.col("best_supplier"),
        pl.col("supplier_combination").alias("combination_id"),
        pl.col("item_number").alias("item_id"),
    ).to_dicts()
    logger.debug(f"Found {len(combination_item_offers_data)} combination item offers")

    logger.info("Inserting combination item offers into database")
    stmt = insert(tables["combination_item_offers"]).values(combination_item_offers_data)
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

    logger.info("Data loading completed successfully")


# Flagged tables
def create_tables_flagged(table_name_prefix, metadata, engine, create_tables=True):
    """Create database tables for flagged data."""

    # --- Table Definitions (SQLAlchemy Core) ---
    flagged_table = Table(
        f"{table_name_prefix}flagged_data",
        metadata,
        Column(
            "id",
            BigInteger,
            Identity(),
            primary_key=True,
            autoincrement=True,
            comment="Unique identifier for the flagged data record.",
        ),
        Column("item_number", String, comment="Item number identifier."),
        Column("items_price", Numeric, comment="Price of the item."),
        Column("qty", Numeric, comment="Quantity of the item."),
        Column("uom", String, comment="Unit of Measure for the item."),
        Column("suppliers", String, comment="Name of the supplier."),
        Column("currency", String, comment="Currency of the price."),
        Column(
            "weight",
            Numeric,
            comment="The weight of the item (relative price importance).",
        ),
        Column(
            "supplier_weight_sum", Numeric, comment="Sum of weights for the supplier."
        ),
        Column(
            "median_price_weight",
            Numeric,
            nullable=True,
            comment="Median price weight calculation.",
        ),
        Column(
            "supplier_weight_sum_qty",
            Numeric,
            comment="Supplier weight sum considering quantity.",
        ),
        Column("total_cost_per_vendor", Numeric, comment="Total cost for the vendor."),
        Column("qflag", BigInteger, comment="Quality flag based on total cost (rank)."),
        Column(
            "valid_offer_count",
            BigInteger,
            comment="Count of valid offers from the supplier.",
        ),
        Column(
            "common_item_count",
            BigInteger,
            comment="Count of items common across all suppliers.",
        ),
        Column(
            "common_items",
            String,
            comment="Indicator if item is common across suppliers (+ or ~).",
        ),
        Column(
            "qflag_weight", BigInteger, comment="Quality flag based on weight (rank)."
        ),
        Column(
            "qflag_w_qty",
            BigInteger,
            comment="Quality flag based on weight with quantity (rank).",
        ),
        Column(
            "qflag_cover",
            BigInteger,
            comment="Quality flag based on offer coverage (rank).",
        ),
        comment="Stores flagged data for bid evaluation with quality indicators.",
    )
    # Create tables
    if create_tables:
        metadata.create_all(engine)
    return {"flagged_data": flagged_table}


def pre_process_data_flagged(df: pd.DataFrame):
    """Pre-process data for flagged tables.
    
    Args:
        df: Input DataFrame containing bid evaluation data
        
    Returns:
        StringIO object containing processed CSV data
    """
    logger.info("Starting data pre-processing for flagged data")
    logger.debug(f"Input DataFrame shape: {df.shape}")

    # Rename columns to lowercase with underscores
    df = df.rename(columns={
        "Item number": "item_number",
        "Items price": "items_price",
        "QTY": "qty",
        "UoM": "uom",
        "Suppliers": "suppliers",
        "Currency": "currency",
        "Weight": "weight",
        "Supplier Weight Sum": "supplier_weight_sum",
        "Supplier Weight Sum QTY": "supplier_weight_sum_qty",
        "Total cost per Vendor": "total_cost_per_vendor",
        "Valid_Offer_Count": "valid_offer_count",
        "Common Item Count": "common_item_count",
        "Common Items": "common_items",
        "qflag_Weight": "qflag_weight",
        "qflag_W_QTY": "qflag_w_qty",
        "qflagCover": "qflag_cover"
    })

    # Calculate median prices and weights
    logger.info("Calculating median prices and weights")
    median_prices = df.groupby("item_number")["items_price"].median()
    total_median_price = median_prices.sum()
    df["weight"] = df["item_number"].map(median_prices) / total_median_price

    # Calculate supplier weight sums
    logger.info("Calculating supplier weight sums")
    mask = df["items_price"] != 0
    supplier_weight_sum = df[mask].groupby("suppliers")["weight"].sum()
    df["supplier_weight_sum"] = df["suppliers"].map(supplier_weight_sum)

    # Calculate supplier weight sums with quantity
    logger.info("Calculating supplier weight sums with quantity")
    df["supplier_weight_sum_qty"] = df["supplier_weight_sum"] * df["qty"]

    # Calculate total cost per vendor
    logger.info("Calculating total cost per vendor")
    df["total_cost_per_vendor"] = df["items_price"] * df["qty"]

    # Calculate quality flags - handle NaN and inf values
    logger.info("Calculating quality flags")
    # Replace inf with NaN and then fill NaN with 0 before ranking
    df["qflag"] = (
        df["total_cost_per_vendor"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    # Calculate vendor offer counts
    logger.info("Calculating vendor offer counts")
    vendor_offer_counts = df[df["items_price"] > 0].groupby("suppliers")["item_number"].count().reset_index()
    vendor_offer_counts.rename(columns={"item_number": "valid_offer_count"}, inplace=True)
    df = df.merge(vendor_offer_counts, on="suppliers", how="left")
    df["valid_offer_count"] = df["valid_offer_count"].fillna(0).astype(int)

    # Process common items
    logger.info("Processing common items")
    df_filtered = df[df["items_price"].notnull()]
    supplier_item_sets = df_filtered.groupby("suppliers")["item_number"].apply(set)
    common_items = set.intersection(*supplier_item_sets)
    common_item_count = len(common_items)
    df["common_item_count"] = common_item_count
    df["common_items"] = df["item_number"].apply(lambda x: "+" if x in common_items else "~")
    logger.debug(f"Found {common_item_count} common items")

    # Calculate additional quality flags - handle NaN and inf values
    logger.info("Calculating additional quality flags")
    df["qflag_weight"] = (
        df["supplier_weight_sum"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    df["qflag_w_qty"] = (
        df["supplier_weight_sum_qty"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    df["qflag_cover"] = (
        df["valid_offer_count"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    # Write output
    logger.info("Writing processed data to CSV")
    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    logger.info("Pre-processing completed successfully")
    return output


def load_data_flagged(csv_file, tables, engine):
    """Process CSV data and fill flagged data table.

    Args:
        csv_file: File object or path to the CSV file containing bid evaluation data
        tables: Dictionary of SQLAlchemy Table objects
        engine: SQLAlchemy engine instance
    """
    logger.info("Starting data loading for flagged data")
    
    df = pd.read_csv(csv_file)
    logger.debug(f"Loaded CSV data with shape: {df.shape}")

    # Convert to list of dictionaries for insertion
    flagged_data = df.to_dict("records")
    logger.debug(f"Prepared {len(flagged_data)} records for insertion")

    # Insert data into the flagged_data table
    logger.info("Inserting flagged data into database")
    stmt = insert(tables["flagged_data"]).values(flagged_data)
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

    logger.info("Flagged data loading completed successfully")