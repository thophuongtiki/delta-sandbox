# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "altair-nx==1.3",
#     "altair==5.5.0",
#     "marimo",
#     "networkx==3.5",
#     "polars==1.30.0",
# ]
# ///

import marimo

__generated_with = "0.13.15"
app = marimo.App(width="columns", app_title="Selection Performance")


@app.cell(column=0)
def _():
    import os

    import marimo as mo

    import polars as pl
    import altair as alt
    pl.Config.set_engine_affinity(engine="streaming")
    alt.data_transformers.disable_max_rows

    import networkx as nx
    from networkx.algorithms import bipartite

    import altair_nx as anx

    return mo, os, pl


@app.cell
def _(os, pl):
    SUBCATES = [
        'Sport - Travel',
        'Home Living',
        'Fashion',
        'ITC',
        'Mom - Baby',
        'Grocery',
        'Large Appliances',
        'Book & Office Supplies',
        'Automotive & Motorcycle',
        'Small Appliances',
        'Health - Beauty',
        'Phones - Tablets'
    ]

    EVENTS = ["view","add_to_cart","checkout"]

    BUCKET_NAME = os.getenv("BUCKET_NAME")
    OBJECT_NAME = os.getenv("OBJECT_NAME")

    def read_data(subcates: list[str], events: list[str]) -> tuple[pl.DataFrame, pl.DataFrame]:
        """ 
        Return: Product DataFrame and Event DataFrame
        """

        _DIR = f"gs://{BUCKET_NAME}/{OBJECT_NAME}"

        _PRODUCT_COLS = [
            "product_id",
            "product_name",
            "sub_cate_report",
            "brand",
            "cate2",
            "deepest_cate_name",
            "business_type",
            "seller_supplier"
        ]

        full_df = (
            pl.scan_parquet(f"{_DIR}/graph.parquet")
            .filter(pl.col("event_name").is_in(events) & pl.col("sub_cate_report").is_in(subcates))
            .with_columns(
                product_id=pl.col("product_id").cast(pl.String),
                customer_id=pl.lit("c")+pl.col("customer_id").cast(pl.String)
            )
            .collect()
        )

        product_df = (
            full_df
            .select(_PRODUCT_COLS)
            .unique()
        )

        event_df = (
            full_df        
            .select("product_id","customer_id","event_name","n_events")
            .rename({"n_events":"weight"})
        )

        return product_df, event_df
    return EVENTS, SUBCATES, read_data


@app.function
def draw_graph():
    pass


@app.cell(column=1)
def _(EVENTS, SUBCATES, mo):
    form = (
        mo.md(
            '''
        **Select subcate(s):**

        {filtered_subcates}

        **Select event:**

        {filtered_events}
        '''
        )
        .batch(
            filtered_subcates=mo.ui.multiselect(options=sorted(SUBCATES), label="Select subcates: "),
            filtered_events=mo.ui.dropdown(options=EVENTS, label="Select event: ")
        )
        .form(show_clear_button=True, bordered=False)
    )
    return (form,)


@app.cell
def _(form):
    form
    return


@app.cell
def _(form, pl, read_data):
    event_df = pl.DataFrame()
    product_df = pl.DataFrame()

    filtered_subcates = form.value["filtered_subcates"]
    filtered_events = form.value["filtered_events"]

    if len(filtered_subcates) != 0 and len(filtered_events) > 0 :
        product_df, event_df = read_data(subcates=filtered_subcates, events=[filtered_events])
    
    return (product_df,)


@app.cell
def _(mo, product_df):
    full_ranking_table = mo.ui.table(product_df, label="Event Table", pagination=True, page_size=20)
    full_ranking_table
    return


if __name__ == "__main__":
    app.run()
