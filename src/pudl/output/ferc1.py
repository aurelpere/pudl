"""A collection of denormalized FERC assets and helper functions."""
import json
from typing import Any, NamedTuple, Self

import networkx as nx
import numpy as np
import pandas as pd
from dagster import AssetIn, AssetsDefinition, Field, Mapping, asset
from matplotlib import pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout
from pydantic import BaseModel, validator

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_utilities_ferc1(
    plants_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """A denormalized table containing FERC plant and utility names and IDs."""
    return pd.merge(plants_ferc1, utilities_ferc1, on="utility_id_ferc1")


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_steam_ferc1(
    denorm_plants_utilities_ferc1: pd.DataFrame, plants_steam_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Select and joins some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.
    Also calculates ``capacity_factor`` (based on ``net_generation_mwh`` &
    ``capacity_mw``)

    Args:
        denorm_plants_utilities_ferc1: Denormalized dataframe of FERC Form 1 plants and
            utilities data.
        plants_steam_ferc1: The normalized FERC Form 1 steam table.

    Returns:
        A DataFrame containing useful fields from the FERC Form 1 steam table.
    """
    steam_df = (
        plants_steam_ferc1.merge(
            denorm_plants_utilities_ferc1,
            on=["utility_id_ferc1", "plant_name_ferc1"],
            how="left",
        )
        .assign(
            capacity_factor=lambda x: x.net_generation_mwh / (8760 * x.capacity_mw),
            opex_fuel_per_mwh=lambda x: x.opex_fuel / x.net_generation_mwh,
            opex_total_nonfuel=lambda x: x.opex_production_total
            - x.opex_fuel.fillna(0),
            opex_nonfuel_per_mwh=lambda x: np.where(
                x.net_generation_mwh > 0,
                x.opex_total_nonfuel / x.net_generation_mwh,
                np.nan,
            ),
        )
        .pipe(calc_annual_capital_additions_ferc1)
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_id_ferc1",
                "plant_name_ferc1",
            ],
        )
    )
    return steam_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_small_ferc1(
    plants_small_ferc1: pd.DataFrame, denorm_plants_utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe related to the FERC Form 1 small plants."""
    plants_small_df = (
        plants_small_ferc1.merge(
            denorm_plants_utilities_ferc1,
            on=["utility_id_ferc1", "plant_name_ferc1"],
            how="left",
        )
        .assign(
            opex_total=lambda x: (
                x[["opex_fuel", "opex_maintenance", "opex_operations"]]
                .fillna(0)
                .sum(axis=1)
            ),
            opex_total_nonfuel=lambda x: (x.opex_total - x.opex_fuel.fillna(0)),
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_name_ferc1",
                "record_id",
            ],
        )
    )

    return plants_small_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_hydro_ferc1(
    plants_hydro_ferc1: pd.DataFrame, denorm_plants_utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe related to the FERC Form 1 hydro plants."""
    plants_hydro_df = (
        plants_hydro_ferc1.merge(
            denorm_plants_utilities_ferc1,
            on=["utility_id_ferc1", "plant_name_ferc1"],
            how="left",
        )
        .assign(
            capacity_factor=lambda x: (x.net_generation_mwh / (8760 * x.capacity_mw)),
            opex_total_nonfuel=lambda x: x.opex_total,
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_name_ferc1",
                "record_id",
            ],
        )
    )
    return plants_hydro_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_pumped_storage_ferc1(
    plants_pumped_storage_ferc1: pd.DataFrame,
    denorm_plants_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a dataframe of FERC Form 1 Pumped Storage plant data."""
    pumped_storage_df = (
        plants_pumped_storage_ferc1.merge(
            denorm_plants_utilities_ferc1,
            on=["utility_id_ferc1", "plant_name_ferc1"],
            how="left",
        )
        .assign(
            capacity_factor=lambda x: x.net_generation_mwh / (8760 * x.capacity_mw),
            opex_total_nonfuel=lambda x: x.opex_total,
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_name_ferc1",
                "record_id",
            ],
        )
    )
    return pumped_storage_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_fuel_ferc1(
    fuel_ferc1: pd.DataFrame, denorm_plants_utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe related to FERC Form 1 fuel information.

    This function pulls the FERC Form 1 fuel data, and joins in the name of the
    reporting utility, as well as the PUDL IDs for that utility and the plant, allowing
    integration with other PUDL tables. Useful derived values include:

    * ``fuel_consumed_mmbtu`` (total fuel heat content consumed)
    * ``fuel_consumed_total_cost`` (total cost of that fuel)

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.

    Returns:
        A DataFrame containing useful FERC Form 1 fuel
        information.
    """
    fuel_df = (
        fuel_ferc1.assign(
            fuel_consumed_mmbtu=lambda x: x["fuel_consumed_units"]
            * x["fuel_mmbtu_per_unit"],
            fuel_consumed_total_cost=lambda x: x["fuel_consumed_units"]
            * x["fuel_cost_per_unit_burned"],
        )
        .merge(
            denorm_plants_utilities_ferc1,
            on=["utility_id_ferc1", "plant_name_ferc1"],
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_name_ferc1",
            ],
        )
    )
    return fuel_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_purchased_power_ferc1(
    purchased_power_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    purchased_power_df = purchased_power_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "seller_name",
            "record_id",
        ],
    )
    return purchased_power_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plant_in_service_ferc1(
    plant_in_service_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a dataframe of FERC Form 1 Electric Plant in Service data."""
    pis_df = plant_in_service_ferc1.merge(utilities_ferc1, on="utility_id_ferc1").pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
        ],
    )
    return pis_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_balance_sheet_assets_ferc1(
    balance_sheet_assets_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 balance sheet assets data."""
    denorm_balance_sheet_assets_ferc1 = balance_sheet_assets_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "asset_type",
        ],
    )
    return denorm_balance_sheet_assets_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_balance_sheet_liabilities_ferc1(
    balance_sheet_liabilities_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 balance_sheet liabilities data."""
    denorm_balance_sheet_liabilities_ferc1 = balance_sheet_liabilities_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "liability_type",
        ],
    )
    return denorm_balance_sheet_liabilities_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_cash_flow_ferc1(
    cash_flow_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 cash flow data."""
    denorm_cash_flow_ferc1 = cash_flow_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "amount_type",
        ],
    )
    return denorm_cash_flow_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_depreciation_amortization_summary_ferc1(
    depreciation_amortization_summary_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 depreciation amortization data."""
    denorm_depreciation_amortization_summary_ferc1 = (
        depreciation_amortization_summary_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "plant_function",
                "ferc_account_label",
            ],
        )
    )
    return denorm_depreciation_amortization_summary_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_energy_dispositions_ferc1(
    electric_energy_dispositions_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 energy dispositions data."""
    denorm_electric_energy_dispositions_ferc1 = (
        electric_energy_dispositions_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "energy_disposition_type",
            ],
        )
    )
    return denorm_electric_energy_dispositions_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_energy_sources_ferc1(
    electric_energy_sources_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_energy_sources_ferc1 = electric_energy_sources_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "energy_source_type",
        ],
    )
    return denorm_electric_energy_sources_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_operating_expenses_ferc1(
    electric_operating_expenses_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_operating_expenses_ferc1 = electric_operating_expenses_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "expense_type",
        ],
    )
    return denorm_electric_operating_expenses_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_operating_revenues_ferc1(
    electric_operating_revenues_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_operating_revenues_ferc1 = electric_operating_revenues_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "revenue_type",
        ],
    )
    return denorm_electric_operating_revenues_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_plant_depreciation_changes_ferc1(
    electric_plant_depreciation_changes_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_plant_depreciation_changes_ferc1 = (
        electric_plant_depreciation_changes_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "depreciation_type",
                "plant_status",
                "utility_type",
            ],
        )
    )
    return denorm_electric_plant_depreciation_changes_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_plant_depreciation_functional_ferc1(
    electric_plant_depreciation_functional_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_plant_depreciation_functional_ferc1 = (
        electric_plant_depreciation_functional_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "plant_function",
                "plant_status",
                "utility_type",
            ],
        )
    )
    return denorm_electric_plant_depreciation_functional_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electricity_sales_by_rate_schedule_ferc1(
    electricity_sales_by_rate_schedule_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electricity_sales_by_rate_schedule_ferc1 = (
        electricity_sales_by_rate_schedule_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
            ],
        )
    )
    return denorm_electricity_sales_by_rate_schedule_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_income_statement_ferc1(
    income_statement_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_income_statement_ferc1 = income_statement_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "utility_type",
            "income_type",
        ],
    )
    return denorm_income_statement_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_other_regulatory_liabilities_ferc1(
    other_regulatory_liabilities_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_other_regulatory_liabilities_ferc1 = (
        other_regulatory_liabilities_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
            ],
        )
    )
    return denorm_other_regulatory_liabilities_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_retained_earnings_ferc1(
    retained_earnings_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_retained_earnings_ferc1 = retained_earnings_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "earnings_type",
        ],
    )
    return denorm_retained_earnings_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_transmission_statistics_ferc1(
    transmission_statistics_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_transmission_statistics_ferc1 = transmission_statistics_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
        ],
    )
    return denorm_transmission_statistics_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_utility_plant_summary_ferc1(
    utility_plant_summary_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_utility_plant_summary_ferc1 = utility_plant_summary_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "utility_type",
            "utility_plant_asset_type",
        ],
    )
    return denorm_utility_plant_summary_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_all_ferc1(
    denorm_plants_steam_ferc1: pd.DataFrame,
    denorm_plants_small_ferc1: pd.DataFrame,
    denorm_plants_hydro_ferc1: pd.DataFrame,
    denorm_plants_pumped_storage_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Combine the steam, small generators, hydro, and pumped storage tables.

    While this table may have many purposes, the main one is to prepare it for
    integration with the EIA Master Unit List (MUL). All subtables included in this
    output table must have pudl ids. Table prepping involves ensuring that the
    individual tables can merge correctly (like columns have the same name) both with
    each other and the EIA MUL.
    """
    # Prep steam table
    logger.debug("prepping steam table")
    steam_df = denorm_plants_steam_ferc1.rename(columns={"opex_plants": "opex_plant"})

    # Prep hydro tables (Add this to the meta data later)
    logger.debug("prepping hydro tables")
    hydro_df = denorm_plants_hydro_ferc1.rename(
        columns={"project_num": "ferc_license_id"}
    )
    pump_df = denorm_plants_pumped_storage_ferc1.rename(
        columns={"project_num": "ferc_license_id"}
    )

    # Combine all the tables together
    logger.debug("combining all tables")
    all_df = (
        pd.concat([steam_df, denorm_plants_small_ferc1, hydro_df, pump_df])
        .rename(
            columns={
                "fuel_cost": "total_fuel_cost",
                "fuel_mmbtu": "total_mmbtu",
                "opex_fuel_per_mwh": "fuel_cost_per_mwh",
                "primary_fuel_by_mmbtu": "fuel_type_code_pudl",
            }
        )
        .replace({"": np.nan})
    )

    return all_df


@asset(
    io_manager_key="pudl_sqlite_io_manager",
    config_schema={
        "thresh": Field(
            float,
            default_value=0.5,
            description=(
                "Minimum fraction of fuel (cost and mmbtu) required in order for a "
                "plant to be assigned a primary fuel. Must be between 0.5 and 1.0. "
                "Default value is 0.5."
            ),
        )
    },
    compute_kind="Python",
)
def denorm_fuel_by_plant_ferc1(
    context,
    fuel_ferc1: pd.DataFrame,
    denorm_plants_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize FERC fuel data by plant for output.

    This is mostly a wrapper around
    :func:`pudl.analysis.classify_plants_ferc1.fuel_by_plant_ferc1`
    which calculates some summary values on a per-plant basis (as indicated
    by ``utility_id_ferc1`` and ``plant_name_ferc1``) related to fuel
    consumption.

    Args:
        context: Dagster context object
        fuel_ferc1: Normalized FERC fuel table.
        denorm_plants_utilities_ferc1: Denormalized table of FERC1 plant & utility IDs.

    Returns:
        A DataFrame with fuel use summarized by plant.
    """

    def drop_other_fuel_types(df):
        """Internal function to drop other fuel type.

        Fuel type other indicates we didn't know how to categorize the reported fuel
        type, which leads to records with incomplete and unsable data.
        """
        return df[df.fuel_type_code_pudl != "other"].copy()

    thresh = context.op_config["thresh"]
    # The existing function expects `fuel_type_code_pudl` to be an object, rather than
    # a category. This is a legacy of pre-dagster code, and we convert here to prevent
    # further retooling in the code-base.
    fuel_ferc1["fuel_type_code_pudl"] = fuel_ferc1["fuel_type_code_pudl"].astype(str)

    fuel_categories = list(
        pudl.transform.ferc1.FuelFerc1TableTransformer()
        .params.categorize_strings["fuel_type_code_pudl"]
        .categories.keys()
    )

    fbp_df = (
        fuel_ferc1.pipe(drop_other_fuel_types)
        .pipe(
            pudl.analysis.classify_plants_ferc1.fuel_by_plant_ferc1,
            fuel_categories=fuel_categories,
            thresh=thresh,
        )
        .pipe(pudl.analysis.classify_plants_ferc1.revert_filled_in_float_nulls)
        .pipe(pudl.analysis.classify_plants_ferc1.revert_filled_in_string_nulls)
        .merge(
            denorm_plants_utilities_ferc1, on=["utility_id_ferc1", "plant_name_ferc1"]
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_name_ferc1",
            ],
        )
    )
    return fbp_df


###########################################################################
# HELPER FUNCTIONS
###########################################################################


def calc_annual_capital_additions_ferc1(
    steam_df: pd.DataFrame, window: int = 3
) -> pd.DataFrame:
    """Calculate annual capital additions for FERC1 steam records.

    Convert the capex_total column into annual capital additons the
    `capex_total` column is the cumulative capital poured into the plant over
    time. This function takes the annual difference should generate the annual
    capial additions. It also want generates a rolling average, to smooth out
    the big annual fluxuations.

    Args:
        steam_df: result of `prep_plants_ferc()`
        window: number of years for window to generate rolling average. Argument for
            :func:`pudl.helpers.generate_rolling_avg`

    Returns:
        Augemented version of steam_df with two additional columns:
        ``capex_annual_addition`` and ``capex_annual_addition_rolling``.
    """
    idx_steam_no_date = ["utility_id_ferc1", "plant_id_ferc1"]
    # we need to sort the df so it lines up w/ the groupby
    steam_df = steam_df.assign(
        report_date=lambda x: pd.to_datetime(x.report_year, format="%Y")
    ).sort_values(idx_steam_no_date + ["report_date"])
    steam_df = steam_df.assign(
        capex_wo_retirement_total=lambda x: x.capex_equipment.fillna(0)
        + x.capex_land.fillna(0)
        + x.capex_structures.fillna(0)
    )
    # we group on everything but the year so the groups are multi-year unique
    # plants the shift happens within these multi-year plant groups
    steam_df["capex_total_shifted"] = steam_df.groupby(idx_steam_no_date)[
        ["capex_wo_retirement_total"]
    ].shift()
    steam_df = steam_df.assign(
        capex_annual_addition=lambda x: x.capex_wo_retirement_total
        - x.capex_total_shifted
    )

    addts = pudl.helpers.generate_rolling_avg(
        steam_df,
        group_cols=idx_steam_no_date,
        data_col="capex_annual_addition",
        window=window,
    )
    steam_df_w_addts = pd.merge(
        steam_df,
        addts[
            idx_steam_no_date
            + [
                "report_date",
                "capex_wo_retirement_total",
                "capex_annual_addition_rolling",
            ]
        ],
        on=idx_steam_no_date + ["report_date", "capex_wo_retirement_total"],
        how="left",
    ).assign(
        capex_annual_per_mwh=lambda x: x.capex_annual_addition / x.net_generation_mwh,
        capex_annual_per_mw=lambda x: x.capex_annual_addition / x.capacity_mw,
        capex_annual_per_kw=lambda x: x.capex_annual_addition / x.capacity_mw / 1000,
        capex_annual_per_mwh_rolling=lambda x: x.capex_annual_addition_rolling
        / x.net_generation_mwh,
        capex_annual_per_mw_rolling=lambda x: x.capex_annual_addition_rolling
        / x.capacity_mw,
    )

    steam_df_w_addts = add_mean_cap_additions(steam_df_w_addts)
    # bb tests for volumne of negative annual capex
    neg_cap_addts = len(
        steam_df_w_addts[steam_df_w_addts.capex_annual_addition_rolling < 0]
    ) / len(steam_df_w_addts)
    neg_cap_addts_mw = (
        steam_df_w_addts[
            steam_df_w_addts.capex_annual_addition_rolling < 0
        ].net_generation_mwh.sum()
        / steam_df_w_addts.net_generation_mwh.sum()
    )
    message = (
        f"{neg_cap_addts:.02%} records have negative capitial additions"
        f": {neg_cap_addts_mw:.02%} of capacity"
    )
    if neg_cap_addts > 0.1:
        logger.warning(message)
    else:
        logger.info(message)
    return steam_df_w_addts.drop(
        columns=[
            "report_date",
            "capex_total_shifted",
            "capex_annual_addition_gen_mean",
            "capex_annual_addition_gen_std",
            "capex_annual_addition_diff_mean",
        ]
    )


def add_mean_cap_additions(steam_df):
    """Add mean capital additions over lifetime of plant."""
    idx_steam_no_date = ["utility_id_ferc1", "plant_id_ferc1"]
    gb_cap_an = steam_df.groupby(idx_steam_no_date)[["capex_annual_addition"]]
    # calcuate the standard deviatoin of each generator's capex over time
    df = (
        steam_df.merge(
            gb_cap_an.std()
            .add_suffix("_gen_std")
            .reset_index()
            .pipe(pudl.helpers.convert_cols_dtypes, "ferc1"),
            how="left",
            on=idx_steam_no_date,
            validate="m:1",
        )
        .merge(
            gb_cap_an.mean()
            .add_suffix("_gen_mean")
            .reset_index()
            .pipe(pudl.helpers.convert_cols_dtypes, "ferc1"),
            how="left",
            on=idx_steam_no_date,
            validate="m:1",
        )
        .assign(
            capex_annual_addition_diff_mean=lambda x: x.capex_annual_addition
            - x.capex_annual_addition_gen_mean,
        )
    )
    return df


#########
# Explode
#########
def exploded_table_asset_factory(
    root_table: str,
    table_names_to_explode: list[str],
    calculation_tolerance: float = 0.05,
    io_manager_key: str | None = None,  # TODO: Add metadata for tables
) -> AssetsDefinition:
    """Create an exploded table based on a set of related input tables."""
    ins: Mapping[str, AssetIn] = {
        "clean_xbrl_metadata_json": AssetIn("clean_xbrl_metadata_json")
    }
    ins |= {table_name: AssetIn(table_name) for table_name in table_names_to_explode}

    @asset(name=f"exploded_{root_table}", ins=ins, io_manager_key=io_manager_key)
    def exploded_tables_asset(
        **kwargs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        clean_xbrl_metadata_json = kwargs["clean_xbrl_metadata_json"]
        tables_to_explode = {
            name: df
            for (name, df) in kwargs.items()
            if name != "clean_xbrl_metadata_json"
        }
        return Exploder(
            table_names=tables_to_explode.keys(),
            root_table=root_table,
            clean_xbrl_metadata_json=clean_xbrl_metadata_json,
        ).boom(
            tables_to_explode=tables_to_explode,
            calculation_tolerance=calculation_tolerance,
        )

    return exploded_tables_asset


def create_exploded_table_assets() -> list[AssetsDefinition]:
    """Create a list of exploded FERC Form 1 assets.

    Returns:
        A list of :class:`AssetsDefinitions` where each asset is an exploded FERC Form 1
        table.
    """
    explosion_args = [
        {
            "root_table": "income_statement_ferc1",
            "table_names_to_explode": [
                "income_statement_ferc1",
                "depreciation_amortization_summary_ferc1",
                "electric_operating_expenses_ferc1",
                "electric_operating_revenues_ferc1",
            ],
            "calculation_tolerance": 0.25,
        },
        {
            "root_table": "balance_sheet_assets_ferc1",
            "table_names_to_explode": [
                "balance_sheet_assets_ferc1",
                "utility_plant_summary_ferc1",
                "plant_in_service_ferc1",
                "electric_plant_depreciation_functional_ferc1",
            ],
            "calculation_tolerance": 0.18,
        },
        {
            "root_table": "balance_sheet_liabilities_ferc1",
            "table_names_to_explode": [
                "balance_sheet_liabilities_ferc1",
                "retained_earnings_ferc1",
            ],
        },
    ]
    return [exploded_table_asset_factory(**kwargs) for kwargs in explosion_args]


exploded_ferc1_assets = create_exploded_table_assets()


class MetadataExploder:
    """Combine a set of inter-related, nested table's metadata."""

    def __init__(self, table_names: list[str]):
        """Instantiate MetadataExploder."""
        self.table_names = table_names

    def boom(self, clean_xbrl_metadata_json: dict):
        """Combine a set of interelated tables metadata for use in :class:`Exploder`.

        Args:
            clean_xbrl_metadata_json: cleaned XRBL metadata.
        """
        tbl_metas = []
        for table_name in self.table_names:
            tbl_meta = (
                pudl.transform.ferc1.FERC1_TFR_CLASSES[table_name](
                    xbrl_metadata_json=clean_xbrl_metadata_json[table_name]
                )
                .xbrl_metadata[
                    [
                        "xbrl_factoid",
                        "calculations",
                        "row_type_xbrl",
                        "xbrl_factoid_original",
                        "intra_table_calc_flag",
                    ]
                ]
                .assign(table_name=table_name)
            )
            tbl_metas.append(tbl_meta)
        return (
            pd.concat(tbl_metas)
            .reset_index(drop=True)
            .pipe(self.redefine_calculations_with_components_out_of_explosion)
        )

    def redefine_calculations_with_components_out_of_explosion(
        self, meta_explode: pd.DataFrame
    ) -> pd.DataFrame:
        """Overwrite the calculations with calculation components not in explosion."""
        calc_explode = convert_calculations_into_calculation_component_table(
            meta_explode
        )
        calc_explode["in_explosion"] = calc_explode.source_tables.apply(
            in_explosion_tables, in_explosion_table_names=self.table_names
        )
        not_in_explosion_xbrl_factoids = list(
            calc_explode.loc[~calc_explode.in_explosion, "xbrl_factoid"].unique()
        )
        # this is a temporary variable. Remove when we migrate the pruning/leaf
        # identification into the Tree/Forest builders. Right now this will be used in
        # Exploder.remove_inter_table_calc_duplication. Because we are changing the
        # metadata, this variable can only be generated BEFORE we reclassify these
        # calculations.
        self.inter_table_components_in_intra_table_calc = calc_explode.loc[
            ~calc_explode.in_explosion, "name"
        ].unique()
        meta_explode.loc[
            meta_explode.xbrl_factoid.isin(not_in_explosion_xbrl_factoids),
            ["calculations", "row_type_xbrl"],
        ] = ("[]", "reported_value")
        return meta_explode


class NodeId(NamedTuple):
    """The source table and XBRL factoid identifying a node in a calculation tree.

    Since NodeId is just a :class:`NamedTuple` a list of NodeId instances can also be
    used to index into a :class:pandas.DataFrame` that uses table names and factoids as
    its index.  This is convenient since many :mod:`networkx` functions and methods
    return iterable containers of graph nodes, which can be turned into lists and used
    directly to index into dataframes.
    """

    source_table: str
    xbrl_factoid: str


class Exploder:
    """Get unique, granular datapoints from a set of related, nested FERC1 tables."""

    def __init__(
        self: Self,
        table_names: list[str],
        root_table: str,
        clean_xbrl_metadata_json: dict[str, Any],
        seeds: list[NodeId] = [],
        tags: pd.DataFrame = pd.DataFrame(),
    ):
        """Instantiate an Exploder class.

        Args:
            table_names: list of table names to explode.
            root_table: the table at the base of the tree of tables_to_explode.
            clean_xbrl_metadata_json: json version of the XBRL-metadata.
            seeds: NodeIds to use as seeds for the calculation forest.
            tags: Additional metadata to merge onto the exploded dataframe.
        """
        self.table_names: list[str] = table_names
        self.root_table: str = root_table
        # I don't think we actually need the metadata exploder to stick around?
        # self.meta_exploder: MetadataExploder = MetadataExploder(self.table_names)
        self.metadata_exploded: pd.DataFrame = MetadataExploder(
            table_names=table_names
        ).boom(clean_xbrl_metadata_json)
        self.calculation_forest: XbrlCalculationForestFerc1 = (
            XbrlCalculationForestFerc1(
                exploded_meta=self.metadata_exploded,
                seeds=seeds,
                tags=tags,
            )
        )

    @property
    def other_dimensions(self) -> list[str]:
        """Get all of the column names for the other dimensions."""
        other_dimensions = []
        for table_name in self.table_names:
            other_dimensions.append(
                pudl.transform.ferc1.FERC1_TFR_CLASSES[
                    table_name
                ]().params.reconcile_table_calculations.subtotal_column
            )
        other_dimensions = [sub for sub in other_dimensions if sub]
        return other_dimensions

    @property
    def exploded_pks(self) -> list[str]:
        """Get the joint primary keys of the exploded tables."""
        pks = []
        for table_name in self.table_names:
            xbrl_factoid_name = pudl.transform.ferc1.FERC1_TFR_CLASSES[
                table_name
            ]().params.xbrl_factoid_name
            pks.append(
                [
                    col
                    for col in pudl.metadata.classes.Resource.from_id(
                        table_name
                    ).schema.primary_key
                    if col != xbrl_factoid_name
                ]
            )
        # Some xbrl_factoid names are the same in more than one table, so we also add
        # table_name here.
        pks = (
            pudl.helpers.dedupe_n_flatten_list_of_lists(pks)
            + ["xbrl_factoid"]
            + ["table_name"]
        )
        return pks

    @property
    def value_col(self) -> str:
        """Get the value column for the exploded tables."""
        value_cols = []
        for table_name in self.table_names:
            value_cols.append(
                pudl.transform.ferc1.FERC1_TFR_CLASSES[
                    table_name
                ]().params.reconcile_table_calculations.column_to_check
            )
        if len(set(value_cols)) != 1:
            raise ValueError(
                "Exploding FERC tables requires tables with only one value column. Got: "
                f"{set(value_cols)}"
            )
        value_col = list(set(value_cols))[0]
        return value_col

    def boom(
        self,
        tables_to_explode: dict[str, pd.DataFrame],
        calculation_tolerance: float = 0.05,
    ) -> pd.DataFrame:
        """Explode a set of nested tables.

        There are five main stages of this process:

        #. Prep all of the individual tables for explosion.
        #. Concatenate all of the tabels together.
        #. Remove duplication in the concatenated exploded table.
        #. Annotate the fine-grained data with additional metadata.
        #. Validate that calculated top-level values are correct.

        Args:
            tables_to_explode: dictionary of table name (key) to transfomed table (value).
            calculation_tolerance: What proportion (0-1) of calculated values are
              allowed to be incorrect without raising an AssertionError.
        """
        exploded = (
            self.initial_explosion_concatenation(tables_to_explode)
            .pipe(self.generate_intertable_calculations)
            .pipe(self.reconcile_intertable_calculations, calculation_tolerance)
            .pipe(self.calculation_forest.leafy_data)
        )

        # REMOVE THE DUPLICATION (old method)
        # exploded = (
        # self.remove_factoids_from_mutliple_tables(tables_to_explode)
        # .pipe(self.remove_totals_from_other_dimensions)
        # .pipe(self.remove_inter_table_calc_duplication)
        # .pipe(remove_intra_table_calculated_values)
        # )

        # Verify that we get the same values for the root nodes using only the input
        # data from the leaf nodes:
        # root_calcs = self.calculation_forest.root_calculations
        # TODO: Validate the root node calculations.
        return exploded

    def initial_explosion_concatenation(
        self, tables_to_explode: dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """Concatenate all of the tables for the explosion.

        Merge in some basic pieces of the each table's metadata and add ``table_name``.
        At this point in the explosion, there will be a lot of duplicaiton in the
        output.
        """
        logger.info("Explode: CONCAT!")
        explosion_tables = []
        # GRAB/PREP EACH TABLE
        for table_name, table_df in tables_to_explode.items():
            xbrl_factoid_name = pudl.transform.ferc1.FERC1_TFR_CLASSES[
                table_name
            ]().params.xbrl_factoid_name
            tbl = table_df.assign(table_name=table_name).rename(
                columns={xbrl_factoid_name: "xbrl_factoid"}
            )
            explosion_tables.append(tbl)

        exploded = pd.concat(explosion_tables)
        # drop any metadata columns coming from the tbls bc we may have edited the
        # metadata df so we want to grab that directly
        meta_idx = ["xbrl_factoid", "table_name"]
        meta_columns = [
            col
            for col in exploded
            if col
            in [
                meta_col
                for meta_col in self.metadata_exploded
                if meta_col not in meta_idx
            ]
        ]
        exploded = exploded.drop(columns=meta_columns).merge(
            self.metadata_exploded,
            how="left",
            on=meta_idx,
            validate="m:1",
        )
        return exploded

    def generate_intertable_calculations(self, exploded: pd.DataFrame) -> pd.DataFrame:
        """Generate calculated values for inter-table calculated factoids.

        This function sums components of calculations for a given factoid when the
        components originate entirely or partially outside of the table. It also
        accounts for components that only sum to a factoid within a particular dimension
        (e.g., for an electric utility or for plants whose plant_function is
        "in_service"). This returns a dataframe with a "calculated_amount" column.

        Args:
            exploded: concatenated tables for table explosion.
        """
        pks_wo_factoid = [col for col in self.exploded_pks if col != "xbrl_factoid"]
        metadata_exploded = self.metadata_exploded
        inter_table_calcs = metadata_exploded[
            (~metadata_exploded.intra_table_calc_flag)
            & (metadata_exploded.row_type_xbrl == "calculated_value")
        ]
        if inter_table_calcs.empty:
            return exploded
        else:
            logger.info(
                f"{self.root_table}: Reconcile inter-table calculations: "
                f"{list(inter_table_calcs.xbrl_factoid.unique())}."
            )

        inter_table_calc_components = (
            convert_calculations_into_calculation_component_table(inter_table_calcs)
            .set_index(["table_name", "xbrl_factoid"])
            .sort_index()
        )
        calculated_dfs = []
        for calc_idx in set(inter_table_calc_components.index):
            parent_table = calc_idx[0]
            calculated_factoid = calc_idx[1]
            logger.info(f"Reconcile calculation for {calculated_factoid}")
            calculation_df = inter_table_calc_components.loc[calc_idx]
            # Remove the correction
            # TODO: check if we can remove this step?
            calculation_df = calculation_df.loc[
                ~calculation_df.name.str.contains("correction")
            ]
            # If each component has one source table (they all should)
            if calculation_df["source_tables"].str.len().all() == 1:
                calculation_df = calculation_df.explode(
                    "source_tables"
                )  # Unpack the list
            subdimension = [
                dim for dim in self.other_dimensions if dim != "utility_type"
            ]
            if len(subdimension) == 1:
                subdimension = subdimension[0]
            elif len(subdimension) > 1:
                raise AssertionError(
                    "Multiple other subdimensions not yet implemented!"
                )

            calc_comp_to_data_rename = {
                "name": "xbrl_factoid",
                "source_tables": "table_name",
                "utility_type": "utility_type",
                "subdimension": subdimension,
            }

            calc_idx_cols = ["name", "source_tables"]
            dim_cols = ["utility_type", "subdimension"]
            for dim in dim_cols:
                if calculation_df[dim].notnull().all():
                    calc_idx_cols.append(dim)
            data_idx_cols = [
                data_col
                for (calc_col, data_col) in calc_comp_to_data_rename.items()
                if calc_col in calc_idx_cols
            ]
            calc_comp_idx = (
                calculation_df.reset_index()
                .set_index(calc_idx_cols)
                .index.rename(calc_comp_to_data_rename)
            )
            # SELECT ONLY THE COMPONENTS
            components = exploded.set_index(data_idx_cols).loc[calc_comp_idx]
            # CALC THE STUFF
            calc_df = (
                pd.merge(
                    components.reset_index(),
                    calculation_df.reset_index()[
                        ["name", "source_tables", "weight"]
                    ].rename(columns=calc_comp_to_data_rename),
                    on=["xbrl_factoid", "table_name"],
                )
                # apply the weight from the calc to convey the sign before summing.
                .assign(calculated_amount=lambda x: x[self.value_col] * x.weight)
                .groupby(pks_wo_factoid, as_index=False, dropna=False)[
                    "calculated_amount"
                ]
                .sum(min_count=1)
                # Assign the name of the 'total' factoid and associate with its parent table.
                .assign(xbrl_factoid=calculated_factoid)
                .assign(table_name=parent_table)
            )
            calculated_dfs.append(calc_df)

        calculated_df = pd.merge(
            exploded,
            pd.concat(calculated_dfs),
            on=self.exploded_pks,
            how="outer",
            validate="1:1",
            indicator=True,
        )

        assert calculated_df[
            (calculated_df._merge == "right_only")
            & (calculated_df[self.value_col].notnull())
        ].empty

        calculated_df = calculated_df.drop(columns=["_merge"])
        # # Force value_col to be a float to prevent any hijinks with calculating differences.
        calculated_df[self.value_col] = calculated_df[self.value_col].astype(float)

        return calculated_df

    def reconcile_intertable_calculations(
        self, calculated_df, calculation_tolerance: float = 0.05
    ):
        """Ensure inter-table calculated values match reported values within a tolerance.

        In addition to checking whether all reported "calculated" values match the output
        of our repaired calculations, this function adds a correction record to the
        dataframe that is included in the calculations so that after the fact the
        calculations match exactly. This is only done when the fraction of records that
        don't match within the tolerances of :func:`numpy.isclose` is below a set
        threshold.

        Note that only calculations which are off by a significant amount result in the
        creation of a correction record. Many calculations are off from the reported values
        by exaclty one dollar, presumably due to rounding errrors. These records typically
        do not fail the :func:`numpy.isclose()` test and so are not corrected.

        Args:
            calculated_df: table with calculated fields
            calculation_tolerance: What proportion (0-1) of calculated values are
              allowed to be incorrect without raising an AssertionError.
        """
        if "calculated_amount" in calculated_df.columns:
            calculated_df = calculated_df.assign(
                abs_diff=lambda x: abs(x[self.value_col] - x.calculated_amount),
                rel_diff=lambda x: np.where(
                    (x[self.value_col] != 0.0),
                    abs(x.abs_diff / x[self.value_col]),
                    np.nan,
                ),
            )

            off_df = calculated_df[
                ~np.isclose(
                    calculated_df.calculated_amount, calculated_df[self.value_col]
                )
                & (calculated_df["abs_diff"].notnull())
            ]
            calculated_values = calculated_df[(calculated_df.abs_diff.notnull())]
            off_ratio = len(off_df) / len(calculated_values)

            if off_ratio > calculation_tolerance:
                raise AssertionError(
                    f"Calculations in {self.root_table} are off by {off_ratio}. Expected tolerance "
                    f"of {calculation_tolerance}."
                )

            # # We'll only get here if the proportion of calculations that are off is acceptable
            if off_ratio > 0:
                logger.info(
                    f"{self.root_table}: has {len(off_df)} ({off_ratio:.02%}) records whose "
                    "calculations don't match. Adding correction records to make calculations "
                    "match reported values."
                )
                corrections = off_df.copy()

                corrections[self.value_col] = (
                    corrections[self.value_col].fillna(0.0)
                    - corrections["calculated_amount"]
                )
                corrections["original_factoid"] = corrections["xbrl_factoid"]
                corrections["xbrl_factoid"] = (
                    corrections["xbrl_factoid"] + "_correction"
                )
                corrections["row_type_xbrl"] = "correction"
                corrections["intra_table_calc_flag"] = False
                corrections["record_id"] = pd.NA

                calculated_df = pd.concat(
                    [calculated_df, corrections], axis="index"
                ).reset_index(drop=True)

            # # If the calculation only has one component (and is therefore exactly equivalent to
            # # a factoid from another table), add the corrections from that table to this
            # # correction and produce one factoid.

            # for calculated_factoid, calculation in set(zip(corrections.xbrl_factoid, corrections.calculations)):
            #     calculation_df = pd.DataFrame(json.loads(calculation))
            #     calculation_df = calculation_df.loc[
            #         ~calculation_df.name.str.contains("correction")
            #     ]
            #     # If only one component in the calculation, add to a list to handle corrections
            #     # differently later.
            #     if len(calculation_df) == 1:
            #         original_fact = calculation_df["name"][0]

            #         # If original fact was also calculated - add source tables here!
            #         if calculated_df.loc[(calculated_df.xbrl_factoid == original_fact)&(calculated_df.row_type_xbrl == "calculated_value")&(calculated_df.table_name.isin(calculation_df.source_tables.values))].any():
            #             # Get corresponding original fact corrections
            #             original_fact_corrections = original_fact + "_correction"
            #             original_corrections_meta = (
            #                 corrections[pks_wo_factoid]
            #                 .assign(xbrl_factoid=original_fact_corrections)
            #                 .assign(table_name=corrections.source_tables)
            #             )
            #             original_corrections_meta.drop(
            #                 columns=["source_tables"], inplace=True
            #             )
            #             pks_corrections = [col for col in original_corrections_meta]
            #             logger.info(original_corrections_meta)
            #             logger.info(pks_corrections)
            #             # TO FIX and FINISH - temporary!
            #             original_corrections = calculated_df.merge(
            #                 original_corrections_meta, on=pks_corrections, how="inner"
            #             )
            #             if not original_corrections.empty:
            #                 logger.warning("Need to merge corrections!!")
            #                 return original_corrections
        #     # original_corrections = original_corrections[original_corrections._merge == "both"]

        return calculated_df

    def remove_factoids_from_mutliple_tables(
        self, exploded, tables_to_explode
    ) -> pd.DataFrame:
        """Remove duplicate factoids that have references in multiple tables."""
        # add the table-level so we know which inter-table duped factoid is downstream
        logger.info("Explode: Remove factoids from multiple tables.")
        exploded = pd.merge(
            exploded,
            get_table_levels(tables_to_explode, self.root_table),
            on="table_name",
            how="left",
            validate="m:1",
        )
        # ensure all tbls have level references
        assert ~exploded.table_level.isnull().any()

        # deal w/ factoids that show up in
        inter_table_facts = (
            exploded[
                [
                    "xbrl_factoid_original",
                    "table_name",
                    "row_type_xbrl",
                    "intra_table_calc_flag",
                    "table_level",
                ]
            ]
            .dropna(subset=["xbrl_factoid_original"])
            .drop_duplicates(["xbrl_factoid_original", "table_name"])
            .sort_values(["xbrl_factoid_original", "table_level"], ascending=False)
        )
        # its the combo of xbrl_factoid_original and table_name that we really care about
        # in terms of dropping bc we want to drop the higher-level/less granular table.
        inter_table_facts_to_drop = inter_table_facts[
            inter_table_facts.duplicated(["xbrl_factoid_original"], keep="first")
        ]
        logger.info(
            f"Explode: Preparing to drop: {inter_table_facts_to_drop.xbrl_factoid_original}"
        )
        # TODO: We need to fill in the other_dimensions columns before doing this check.
        # check to see if there are different values in the values that show up in two tables
        inter_table_ref_check = (
            # these are all the dupes not just the ones we are axing
            exploded[
                exploded.xbrl_factoid_original.isin(
                    inter_table_facts_to_drop.xbrl_factoid_original
                )
            ]
            .groupby(
                [col for col in self.exploded_pks if col != "xbrl_factoid"]
                + ["xbrl_factoid_original"],
                dropna=False,
            )[[self.value_col]]
            .nunique()
        )
        assert inter_table_ref_check[inter_table_ref_check[self.value_col] > 1].empty

        factoid_idx = ["xbrl_factoid_original", "table_name"]
        exploded = exploded.set_index(factoid_idx)
        inter_table_refs = exploded.loc[
            inter_table_facts_to_drop.set_index(factoid_idx).index
        ]
        if len(inter_table_refs) > 1:
            logger.info(
                f"Explode: Dropping {len(inter_table_refs)} ({len(inter_table_refs)/len(exploded):.01%}) inter-table references."
            )
            exploded = exploded.loc[
                exploded.index.difference(
                    inter_table_facts_to_drop.set_index(factoid_idx).index
                )
            ].reset_index()
        else:
            logger.info("Explode: Found no inter-table references. Dropping nothing.")
            # reset the index bc our index rn is tbl name and factoid name og.
            exploded = exploded.reset_index()
        return exploded

    def remove_totals_from_other_dimensions(
        self, exploded: pd.DataFrame
    ) -> pd.DataFrame:
        """Remove the totals from the other dimensions."""
        logger.info("Explode: Interdimensional time.")
        # bc we fill in some of the other dimension columns for
        # ensure we are only taking totals from table_name's that have more than one value
        # for their other dimensions.
        # find the totals from the other dimensions
        exploded = exploded.assign(
            **{
                f"{dim}_nunique": exploded.groupby(["table_name"])[dim].transform(
                    "nunique"
                )
                for dim in self.other_dimensions
            }
        )
        exploded = exploded.assign(
            **{
                f"{dim}_total": (exploded[dim] == "total")
                & (exploded[f"{dim}_nunique"] != 1)
                for dim in self.other_dimensions
            }
        )
        total_mask = (exploded[[f"{dim}_total" for dim in self.other_dimensions]]).any(
            axis=1
        )
        total_len = len(exploded[total_mask])
        logger.info(
            f"Removing {total_len} ({total_len/len(exploded):.1%}) of records which are "
            f"totals of the following dimensions {self.other_dimensions}"
        )
        # remove the totals & drop the cols we used to make em
        drop_cols = [
            f"{dim}{suff}"
            for suff in ["_nunique", "_total"]
            for dim in self.other_dimensions
        ]
        exploded = exploded[~total_mask].drop(columns=drop_cols)
        return exploded

    def remove_inter_table_calc_duplication(
        self, exploded: pd.DataFrame
    ) -> pd.DataFrame:
        """Treat the duplication in the inter-table calculations.

        There are several possible ways to remove the duplication in the inter table calcs.
        See issue #2622. Right now we are doing the simplest option which removes some level
        of detail.
        """
        logger.info("Explode: Doing inter-table calc deduplications stuff.")
        inter_table_components_in_intra_table_calc = list(
            self.meta_exploder.inter_table_components_in_intra_table_calc
        )
        if inter_table_components_in_intra_table_calc:
            logger.info(
                "Explode: Removing intra-table calculation components in inter-table "
                f"calcution ({inter_table_components_in_intra_table_calc})."
            )
            # remove the calcuation components that are a part of an inter-table calculation
            exploded = exploded[
                ~exploded.xbrl_factoid.isin(inter_table_components_in_intra_table_calc)
            ]
        return exploded


def in_explosion_tables(
    source_tables: list[str], in_explosion_table_names: list[str]
) -> bool:
    """Determine if any of a list of source_tables in the list of thre explosion tables.

    Args:
        source_tables: the list of tables. Typically from the ``source_tables`` element
            from an xbrl calculation component
        in_explosion_table_names: list of tables involved in a particular set of
            exploded tables.
    """
    return any([True for tbl in source_tables if tbl in in_explosion_table_names])


def convert_calculations_into_calculation_component_table(
    metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Convert xbrl metadata calculations into a table of calculation components."""
    calc_dfs = []
    for calc, tbl, factoid in zip(
        metadata.calculations, metadata.table_name, metadata.xbrl_factoid
    ):
        calc_dfs.append(
            pd.DataFrame(json.loads(calc)).assign(table_name=tbl, xbrl_factoid=factoid)
        )
    calcs = pd.concat(calc_dfs).reset_index(drop=True)

    return calcs.merge(
        metadata.drop(columns=["calculations"]),
        on=["xbrl_factoid", "table_name"],
        how="left",
    )


def get_table_level(table_name: str, top_table: str) -> int:
    """Get a table level."""
    # we may be able to infer this nesting from the metadata
    table_nesting = {
        "balance_sheet_assets_ferc1": {
            "utility_plant_summary_ferc1": {
                "plant_in_service_ferc1": None,
                "electric_plant_depreciation_functional_ferc1": None,
            },
        },
        "balance_sheet_liabilities_ferc1": {"retained_earnings_ferc1": None},
        "income_statement_ferc1": {
            "depreciation_amortization_summary_ferc1": None,
            "electric_operating_expenses_ferc1": None,
            "electric_operating_revenues_ferc1": None,
        },
    }
    if table_name == top_table:
        level = 1
    elif table_name in table_nesting[top_table].keys():
        level = 2
    elif table_name in pudl.helpers.dedupe_n_flatten_list_of_lists(
        [values.keys() for values in table_nesting[top_table].values()]
    ):
        level = 3
    else:
        raise AssertionError(
            f"AH we didn't find yer table name {table_name} in the nested group of "
            "tables. Be sure all the tables you are trying to explode are related."
        )
    return level


def get_table_levels(tables_to_explode: list[str], top_table: str) -> pd.DataFrame:
    """Get a set of table's level in the explosion.

    Level in this context means where it sits in the tree of the relationship of these
    tables. Level 1 is at the root while the higher numbers are towards the leaves of
    the trees.
    """
    table_levels = {"table_name": [], "table_level": []}
    for table_name in tables_to_explode:
        table_levels["table_name"].append(table_name)
        table_levels["table_level"].append(get_table_level(table_name, top_table))
    return pd.DataFrame(table_levels)


def find_intra_table_components_to_remove(
    inter_table_calc: str, table_name: str
) -> list[str]:
    """Find all xbrl_factoid's within a calc which are native to the source table.

    For all calculations which contain any component's that are natively reported in a
    different table than the calculated factoid, we label those as "inter-table"
    calculations. Sometimes those calculations have components with mix sources - from
    the native table and from other tables. For those mixed-source inter-table
    calculated values, we are going to remove all of the components which are native to
    the source table. This removes some detail but enables us to keep the calculated
    value in the table without any duplication.

    Returns:
        a list of ``xbrl_factoid`` names.
    """
    return [
        component["name"]
        for component in json.loads(inter_table_calc)
        if len(component.get("source_tables")) == 1
        and component.get("source_tables")[0] == table_name
    ]


def remove_intra_table_calculated_values(exploded: pd.DataFrame) -> pd.DataFrame:
    """Remove all of the intra-table calculated values.

    This is assuming that all of these calculated values have been validated as a part
    of the table transform step.
    """
    exploded = exploded[
        (exploded.row_type_xbrl != "calculated_value")
        | (exploded.row_type_xbrl.isnull())
        # keep in the inter-table calced value
        | (
            (exploded.row_type_xbrl == "calculated_value")
            & (~exploded.intra_table_calc_flag)
        )
    ]
    return exploded


################################################################################
# XBRL Calculation Tree
################################################################################


class XbrlCalculationForestFerc1(BaseModel):
    """A class for manipulating groups of hierarchically nested XBRL calculations.

    We expect that the facts reported in high-level FERC tables like
    :ref:`income_statement_ferc1` and :ref:`balance_sheet_assets_ferc1` should be
    calculable from many individually reported granular values, based on the
    calculations encoded in the XBRL Metadata, and that these relationships should have
    a hierarchical tree structure. Several individual values from the higher level
    tables will appear as root nodes at the top of each hierarchy, and the leaves in
    the underlying tree structure are the individually reported non-calculated values
    that make them up. Because the top-level tables have several distinct values in
    them, composed of disjunct sets of reported values, we have a forest (a group of
    several trees) rather than a single tree.

    The information required to build a calculation forest is most readily found in the
    data produced by :meth:`MetadataExploder.boom`  A list of seed nodes can also be
    supplied, indicating which nodes must be present in the resulting forest. This can
    be used to prune irrelevant portions of the overall forest out of the exploded
    metadata. If no seeds are provided, then all of the nodes referenced in the
    exploded_meta input dataframe will be used as seeds.

    This class makes heavy use of :mod:`networkx` to manage the graph that we build
    from calculation relationships.
    """

    exploded_meta: pd.DataFrame
    seeds: list[NodeId] = []
    tags: pd.DataFrame = pd.DataFrame()

    class Config:
        """Allow the class to store a dataframe."""

        arbitrary_types_allowed = True

    @validator("exploded_meta", "tags")
    def ensure_correct_dataframe_index(cls, v):
        """Ensure that dataframe is indexed by table_name and xbrl_factoid."""
        idx_cols = ["table_name", "xbrl_factoid"]
        if v.index.names == idx_cols:
            return v
        missing_idx_cols = [col for col in idx_cols if col not in v.columns]
        if missing_idx_cols:
            raise ValueError(
                f"Exploded metadataframes must be indexed by {idx_cols}, but these "
                f"columns were missing: {missing_idx_cols=}"
            )
        drop = v.index.names is None
        return v.set_index(idx_cols, drop=drop)

    @validator("exploded_meta", "tags")
    def dataframe_has_unique_index(cls, v):
        """Ensure that exploded_meta has a unique index."""
        if not v.index.is_unique:
            raise ValueError("DataFrame has non-unique index values.")
        return v

    @validator("seeds", always=True)
    def seeds_not_empty(cls, v, values):
        """If no seeds are provided, use all nodes in the index of exploded_meta."""
        if v == []:
            logger.info("No seeds provided. Using all nodes in exploded_meta index.")
            v = list(values["exploded_meta"].index)
        return v

    @validator("seeds")
    def seeds_within_bounds(cls, v, values):
        """Ensure that all seeds are present within exploded_meta index."""
        bad_seeds = [seed for seed in v if seed not in values["exploded_meta"].index]
        if bad_seeds:
            raise ValueError(f"Seeds missing from exploded_meta index: {bad_seeds=}")
        return v

    @staticmethod
    def exploded_meta_to_digraph(  # noqa: C901
        exploded_meta: pd.DataFrame,
        tags: pd.DataFrame,
    ) -> nx.DiGraph:
        """Construct a :class:`networkx.DiGraph` of all calculations in exploded_meta.

        - Add all edges implied by the calculations found in exploded_meta.
        - Compile node attributes from exploded_meta and add it the the nodes in the
          forest that has been compiled.
        """
        forest: nx.DiGraph = nx.DiGraph()
        attrs = {}
        for row in exploded_meta.itertuples():
            from_node = NodeId(*row.Index)
            if not attrs.get(from_node, False):
                attrs[from_node] = {}
            if not attrs[from_node].get("xbrl_factoid_original", False):
                attrs[from_node] |= {"xbrl_factoid_original": row.xbrl_factoid_original}
            else:
                assert (
                    attrs[from_node]["xbrl_factoid_original"]
                    == row.xbrl_factoid_original
                )
            try:
                attrs[from_node]["tags"] = dict(tags.loc[from_node])
            except KeyError:
                attrs[from_node]["tags"] = {}
            calcs = json.loads(row.calculations)
            for calc in calcs:
                assert len(calc["source_tables"]) == 1
                to_node = NodeId(calc["source_tables"][0], calc["name"])
                if not attrs.get(to_node, False):
                    attrs[to_node] = {}
                if not attrs[to_node].get("weight", False):
                    attrs[to_node] |= {"weight": calc["weight"]}
                else:
                    if attrs[to_node]["weight"] != calc["weight"]:
                        logger.debug(
                            f"Calculation weights do not match for {to_node}: "
                            f"{attrs[to_node]['weight']} != {calc['weight']}. Using a "
                            "weight of -1.0."
                        )
                        # This fix only applies to the case of a passthrough calculation
                        # where the passthrough has a weight of 1.0 but the value it's
                        # pointing at has a weight of -1.0.
                        assert attrs[to_node]["weight"] * calc["weight"] == -1.0
                        attrs[to_node]["weight"] == -1.0
                try:
                    attrs[to_node]["tags"] = dict(tags.loc[to_node])
                except KeyError:
                    attrs[to_node]["tags"] = {}
                forest.add_edge(from_node, to_node)
        nx.set_node_attributes(forest, attrs)

        # This is a temporary hack. These xbrl_factoid values need to have metadata
        # created and injected by the process_xbrl_metadata() method in the FERC 1
        # table transformers... We created them to refer to data that only appears in
        # the DBF data.
        bad_nodes = [
            NodeId("balance_sheet_assets_ferc1", "special_funds_all"),
            NodeId("balance_sheet_assets_ferc1", "nuclear_fuel"),
        ]
        forest.remove_nodes_from(bad_nodes)

        return forest

    @property
    def full_digraph(self: Self) -> nx.DiGraph:
        """A digraph of all calculations described by the exploded metadata."""
        full_digraph = self.exploded_meta_to_digraph(
            exploded_meta=self.exploded_meta,
            tags=self.tags,
        )
        connected_components = list(
            nx.connected_components(full_digraph.to_undirected())
        )
        logger.debug(
            f"Full digraph contains {len(connected_components)} connected components."
        )
        if not nx.is_directed_acyclic_graph(full_digraph):
            logger.critical(
                "Calculations in Exploded Metadata contain cycles, which is invalid."
            )
        return full_digraph

    @property
    def seeded_digraph(self: Self) -> nx.DiGraph:
        """A digraph of all calculations that contribute to the seed values."""
        seeded_nodes = set()
        for seed in self.seeds:
            seeded_nodes = seeded_nodes.union([seed])
            seeded_nodes = seeded_nodes.union(nx.descendants(self.full_digraph, seed))
        seeded_digraph: nx.DiGraph = self.exploded_meta_to_digraph(
            exploded_meta=self.exploded_meta.loc[list(seeded_nodes)],
            tags=self.tags,
        )
        connected_components = list(
            nx.connected_components(seeded_digraph.to_undirected())
        )
        logger.debug(
            f"Seeded digraph contains {len(connected_components)} connected components."
        )
        return seeded_digraph

    @property
    def forest(self: Self) -> nx.DiGraph:
        """A pruned version of the seeded digraph that should be one or more trees.

        Currently this just retuns the seeded digraph. Any programmatic pruning or
        specific changes to the digraph that are required to make it into a tree will
        be done here.
        """
        forest = self.seeded_digraph
        # Remove any node that has only one parent and one child, and add an edge
        # between its parent and child.
        for node in self.passthroughs:
            parent = list(forest.predecessors(node))
            assert len(parent) == 1
            successors = forest.successors(node)
            assert len(list(successors)) == 2
            child = [
                n
                for n in forest.successors(node)
                if not n.xbrl_factoid.endswith("_correction")
            ]
            assert len(child) == 1
            logger.debug(
                f"Replacing passthrough node {node} with edge from "
                f"{parent[0]} to {child[0]}"
            )
            forest.remove_nodes_from(successors)
            forest.add_edge(parent[0], child[0])

        if not nx.is_forest(forest):
            logger.error(
                "Calculations in Exploded Metadata can not be represented as a forest!"
            )
        connected_components = list(nx.connected_components(forest.to_undirected()))
        logger.debug(
            f"Calculation forest contains {len(connected_components)} connected components."
        )
        return forest

    @staticmethod
    def roots(graph: nx.DiGraph) -> list[NodeId]:
        """Identify all root nodes in a digraph."""
        return [n for n, d in graph.in_degree() if d == 0]

    @property
    def full_digraph_roots(self: Self) -> list[NodeId]:
        """Find all roots in the full digraph described by the exploded metadata."""
        return self.roots(graph=self.full_digraph)

    @property
    def seeded_digraph_roots(self: Self) -> list[NodeId]:
        """Find all roots in the seeded digraph."""
        return self.roots(graph=self.seeded_digraph)

    @property
    def forest_roots(self: Self) -> list[NodeId]:
        """Find all roots in the pruned calculation forest."""
        return self.roots(graph=self.forest)

    @staticmethod
    def leaves(graph: nx.DiGraph) -> list[NodeId]:
        """Identify all leaf nodes in a digraph."""
        return [n for n, d in graph.out_degree() if d == 0]

    @property
    def full_digraph_leaves(self: Self) -> list[NodeId]:
        """All leaf nodes in the full digraph."""
        return self.leaves(graph=self.full_digraph)

    @property
    def seeded_digraph_leaves(self: Self) -> list[NodeId]:
        """All leaf nodes in the seeded digraph."""
        return self.leaves(graph=self.seeded_digraph)

    @property
    def forest_leaves(self: Self) -> list[NodeId]:
        """All leaf nodes in the pruned forest."""
        return self.leaves(graph=self.forest)

    @property
    def orphans(self: Self) -> list[NodeId]:
        """Identify all nodes that appear in metadata but not in the full digraph."""
        nodes = self.full_digraph.nodes
        return [n for n in self.exploded_meta.index if n not in nodes]

    @property
    def pruned(self: Self) -> list[NodeId]:
        """List of all nodes that appear in the DAG but not in the pruned forest."""
        all_nodes = self.full_digraph.nodes
        forest_nodes = self.forest.nodes
        return [n for n in all_nodes if n not in forest_nodes]

    @property
    def stepchildren(self: Self) -> list[NodeId]:
        """All nodes in the seeded digraph that have more than one parent."""
        return [n for n, d in self.seeded_digraph.in_degree() if d > 1]

    @property
    def stepparents(self: Self) -> list[NodeId]:
        """All nodes in the seeded digraph with children having more than one parent."""
        stepchildren = self.stepchildren
        stepparents = set()
        graph = self.seeded_digraph
        for stepchild in stepchildren:
            stepparents = stepparents.union(graph.predecessors(stepchild))
        return list(stepparents)

    @property
    def passthroughs(self: Self) -> list[NodeId]:
        """All nodes in the seeded digraph with a single parent and a single child.

        These nodes can be pruned, hopefully converting the seeded digraph into a
        forest. Note that having a "single child" really means having 2 children, one
        of which is a _correction to the calculation. We verify that the two children
        are one real child node, and one appropriate correction.
        """
        # In theory every node should have only one parent, but just to be safe, since
        # that's not always true right now:
        has_one_parent = {n for n, d in self.seeded_digraph.in_degree() if d == 1}
        # Calculated fields always have both the reported child and a correction that
        # we have added, so having "one" child really means having 2 successor nodes.
        may_have_one_child: set[NodeId] = {
            n for n, d in self.seeded_digraph.out_degree() if d == 2
        }
        # Check that one of these successors is the correction.
        has_one_child = []
        for node in may_have_one_child:
            children = self.seeded_digraph.successors(node)
            for child in children:
                if (node.source_table == child.source_table) and (
                    child.xbrl_factoid == node.xbrl_factoid + "_correction"
                ):
                    has_one_child.append(node)

        return list(has_one_parent.intersection(has_one_child))

    def pprint_calculation_at(self: Self, node_id: NodeId) -> None:
        """Pretty print the calculation associated with a given node."""
        print(
            json.dumps(
                json.loads(self.exploded_meta.at[node_id, "calculations"]), indent=4
            )
        )

    @property
    def leafy_meta(self: Self) -> pd.DataFrame:
        """Identify leaf facts and compile their metadata.

        - identify the root and leaf nodes of those minimal trees
        - adjust the weights associated with the leaf nodes to equal the
          product of the weights of all their ancestors.
        - Set leaf node tags to be the union of all the tags associated
          with all of their ancestors.

        Leafy metadata in the output dataframe includes:

        - The ID of the leaf node itself (this is the index).
        - The ID of the root node the leaf is descended from.
        - What tags the leaf has inherited from its ancestors.
        - The leaf node's xbrl_factoid_original
        - The weight associated with the leaf, in relation to its root.
        """
        # Create a copy of the graph representation since we are going to mutate it.
        forest = self.forest
        leaves = self.forest_leaves
        roots = self.forest_roots
        pruned_forest = nx.DiGraph()
        pruned_forest.add_nodes_from(forest.nodes(data=True))
        pruned_forest.add_edges_from(forest.edges(data=True))

        # Construct a dataframe that links the leaf node IDs to their root nodes:
        leaf_to_root_map = {
            leaf: root
            for leaf in leaves
            for root in roots
            if leaf in nx.descendants(pruned_forest, root)
        }
        leaves_df = pd.DataFrame(list(leaf_to_root_map.keys()))
        roots_df = pd.DataFrame(list(leaf_to_root_map.values())).rename(
            columns={"source_table": "root_table", "xbrl_factoid": "root_xbrl_factoid"}
        )
        leafy_meta = pd.concat([leaves_df, roots_df], axis="columns")

        # Propagate tags and weights to leaf nodes
        leaf_rows = []
        for leaf in leaves:
            leaf_tags = {}
            leaf_weight = pruned_forest.nodes[leaf].get("weight", 1.0)
            for node in nx.ancestors(pruned_forest, leaf):
                # TODO: need to check that there are no conflicts between tags that are
                # being propagated, e.g. if two different ancestors have been tagged
                # rate_base: yes and rate_base: no.
                leaf_tags |= pruned_forest.nodes[node]["tags"]
                # Root nodes have no weight because they don't come from calculations
                # We assign them a weight of 1.0
                if not pruned_forest.nodes[node].get("weight", False):
                    assert node in roots
                    node_weight = 1.0
                else:
                    node_weight = pruned_forest.nodes[node]["weight"]
                leaf_weight *= node_weight

            # Construct a dictionary describing the leaf node and convert it into a
            # single row DataFrame. This makes adding arbitrary tags easy.
            leaf_attrs = {
                "xbrl_factoid_original": pruned_forest.nodes[leaf][
                    "xbrl_factoid_original"
                ],
                "weight": leaf_weight,
                "tags": leaf_tags,
                "source_table": leaf.source_table,
                "xbrl_factoid": leaf.xbrl_factoid,
            }
            leaf_rows.append(pd.json_normalize(leaf_attrs))

        # Combine the two dataframes we've constructed above:
        return (
            pd.merge(leafy_meta, pd.concat(leaf_rows), validate="one_to_one")
            .convert_dtypes()
            .set_index(["source_table", "xbrl_factoid"])
        )

    @property
    def root_calculations(self: Self) -> pd.DataFrame:
        """Produce an exploded metadataframe containing only roots and leaves.

        This dataframe has a format similar to exploded_meta and can be used in
        conjunction with the exploded data to verify that the root values can still
        be correctly calculated from the leaf values.

        """

        def leafy_meta_to_calculations(df: pd.DataFrame) -> str:
            return json.dumps(
                [
                    {
                        "name": row.xbrl_factoid,
                        "weight": float(row.weight),
                        "xbrl_factoid_original": row.xbrl_factoid_original,
                        "source_tables": [row.source_table],
                    }
                    for row in df.itertuples()
                ]
            )

        root_calcs: pd.DataFrame = (
            self.leafy_meta.reset_index()
            .groupby(["root_table", "root_xbrl_factoid"], as_index=False)
            .apply(leafy_meta_to_calculations)
        )
        root_calcs.columns = ["root_table", "root_xbrl_factoid", "calculations"]
        return root_calcs

    @staticmethod
    def plot(graph: nx.DiGraph) -> None:
        """Visualize the calculation forest and its attributes."""
        # Need to make this dynamic / not dependent on particular tables:
        colors = {
            "balance_sheet_assets_ferc1": "red",
            "utility_plant_summary_ferc1": "orange",
            "plant_in_service_ferc1": "yellow",
            "electric_plant_depreciation_functional_ferc1": "green",
        }
        node_color = [colors[node.source_table] for node in graph.nodes]

        pos = graphviz_layout(graph, prog="dot", args='-Grankdir="LR"')
        nx.draw_networkx_nodes(graph, pos, node_color=node_color)
        nx.draw_networkx_edges(graph, pos)
        # The labels are currently unwieldy
        # nx.draw_networkx_labels(nx_forest, pos)
        # Use this to draw everything if/once labels are fixed
        # nx.draw_networkx(nx_forest, pos, node_color=node_color)
        plt.show()

    def plot_full_digraph(self: Self) -> None:
        """Visualize the unpruned DAG."""
        self.plot(self.full_digraph)

    def plot_seeded_digraph(self: Self) -> None:
        """Visualize the pruned forest."""
        self.plot(self.seeded_digraph)

    def plot_forest(self: Self) -> None:
        """Visualize the pruned forest."""
        self.plot(self.forest)

    def leafy_data(self: Self, exploded_data: pd.DataFrame) -> pd.DataFrame:
        """Use the calculation forest to prune the exploded dataframe.

        - Drop all rows that don't correspond to either root or leaf facts.
        - Verify that the reported root values can still be generated by calculations
          that only refer to leaf values.
        - Merge the leafy metadata onto the exploded data, keeping only those rows
          which refer to the leaf facts.
        - Use the leaf weights to adjust the reported data values, and then drop the
          leaf weights.

        This method could either live here, or in the Exploder class, which would also
        have access to exploded_meta, exploded_data, and the calculation forest.

        - Missing connection between UPS and PIS accounts 101_and_106
        - There are a handful of NA values for ``report_year`` and ``utility_id_ferc1``
          because of missing correction records in data.
        - Lingering ``(utility_plant_summary_ferc1,
          utility_plant_in_service_plant_purchased_or_sold_correction)`` in supposedly
          leaf records.
        - Why are ``xbrl_factoid`` and ``table_name`` showing up as tags?
        - No consideration of additional dimensions / primary keys right now.
        - Do we need to keep all the plant in service component columns (e.g. additions,
          retirements) and if so, do they need to be adjusted using the weights too?
        - Lots of cleanup to do as far as column naming, column collisions, which
          columns get kept.
        - Still need to validate the root node calculations.

        """
        leafy_data = pd.merge(
            left=self.leafy_meta.reset_index(),
            right=exploded_data,
            left_on=["source_table", "xbrl_factoid"],
            right_on=["table_name", "xbrl_factoid"],
            how="left",
            validate="one_to_many",
        ).assign(
            starting_balance=lambda x: x.starting_balance * x.weight,
            ending_balance=lambda x: x.starting_balance * x.weight,
        )
        return leafy_data
