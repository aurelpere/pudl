"""Routines for transforming FERC Form 1 data before loading into the PUDL DB.

This module provides a variety of functions that are used in cleaning up the FERC Form 1
data prior to loading into our database. This includes adopting standardized units and
column names, standardizing the formatting of some string values, and correcting data
entry errors which we can infer based on the existing data. It may also include removing
bad data, or replacing it with the appropriate NA values.

"""
import enum
import importlib.resources
import re
import typing
from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Callable
from functools import cached_property
from itertools import combinations
from typing import Protocol

import numpy as np
import pandas as pd
from pydantic import BaseModel, root_validator, validator

import pudl
from pudl.analysis.classify_plants_ferc1 import (
    plants_steam_assign_plant_ids,
    plants_steam_validate_ids,
)
from pudl.extract.ferc1 import TABLE_NAME_MAP
from pudl.helpers import convert_cols_dtypes, get_logger
from pudl.metadata.classes import DataSource, Package
from pudl.metadata.dfs import FERC_DEPRECIATION_LINES
from pudl.settings import Ferc1Settings

logger = get_logger(__name__)


##############################################################################
# Unit converstion parameters
##############################################################################

PERPOUND_TO_PERSHORTTON = dict(
    multiplier=2000.0,
    pattern=r"(.*)_per_lb$",
    repl=r"\1_per_ton",
)
"""Parameters for converting from inverse pounds to inverse short tons."""

CENTS_TO_DOLLARS = dict(
    multiplier=0.01,
    pattern=r"(.*)_cents$",
    repl=r"\1_usd",
)
"""Parameters for converting from cents to dollars."""

PERCF_TO_PERMCF = dict(
    multiplier=1000.0,
    pattern=r"(.*)_per_cf$",
    repl=r"\1_per_mcf",
)
"""Parameters for converting from inverse cubic feet to inverse 1000s of cubic feet."""

PERGALLON_TO_PERBARREL = dict(
    multiplier=42.0,
    pattern=r"(.*)_per_gal",
    repl=r"\1_per_bbl",
)
"""Parameters for converting from inverse gallons to inverse barrels."""

PERKW_TO_PERMW = dict(
    multiplier=1000.0,
    pattern=r"(.*)_per_kw$",
    repl=r"\1_per_mw",
)
"""Parameters for converting column units from per kW to per MW."""

PERKWH_TO_PERMWH = dict(
    multiplier=1000.0,
    pattern=r"(.*)_per_kwh$",
    repl=r"\1_per_mwh",
)
"""Parameters for converting column units from per kWh to per MWh."""

KWH_TO_MWH = dict(
    multiplier=1e-3,
    pattern=r"(.*)_kwh$",
    repl=r"\1_mwh",
)
"""Parameters for converting column units from kWh to MWh."""

BTU_TO_MMBTU = dict(
    multiplier=1e-6,
    pattern=r"(.*)_btu(.*)$",
    repl=r"\1_mmbtu\2",
)
"""Parameters for converting column units from BTU to MMBTU."""

PERBTU_TO_PERMMBTU = dict(
    multiplier=1e6,
    pattern=r"(.*)_per_btu$",
    repl=r"\1_per_mmbtu",
)
"""Parameters for converting column units from BTU to MMBTU."""

BTU_PERKWH_TO_MMBTU_PERMWH = dict(
    multiplier=(1e-6 * 1000.0),
    pattern=r"(.*)_btu_per_kwh$",
    repl=r"\1_mmbtu_per_mwh",
)
"""Parameters for converting column units from BTU/kWh to MMBTU/MWh."""

##############################################################################
# Valid ranges to impose on various columns
##############################################################################

VALID_PLANT_YEARS = {
    "lower_bound": 1850,
    "upper_bound": max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
}
"""Valid range of years for power plant construction."""

VALID_COAL_MMBTU_PER_TON = {
    "lower_bound": 6.5,
    "upper_bound": 29.0,
}
"""Valid range for coal heat content, taken from the EIA-923 instructions.

Lower bound is for waste coal. Upper bound is for bituminous coal.

https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

VALID_COAL_USD_PER_MMBTU = {
    "lower_bound": 0.5,
    "upper_bound": 7.5,
}
"""Historical coal price range from the EIA-923 Fuel Receipts and Costs table."""

VALID_GAS_MMBTU_PER_MCF = {
    "lower_bound": 0.3,
    "upper_bound": 3.3,
}
"""Valid range for gaseous fuel heat content, taken from the EIA-923 instructions.

Lower bound is for landfill gas. Upper bound is for "other gas".  Blast furnace gas
(which has very low heat content) is effectively excluded.

https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

VALID_GAS_USD_PER_MMBTU = {
    "lower_bound": 1.0,
    "upper_bound": 35.0,
}
"""Historical natural gas price range from the EIA-923 Fuel Receipts and Costs table."""

VALID_OIL_MMBTU_PER_BBL = {
    "lower_bound": 3.0,
    "upper_bound": 6.9,
}
"""Valid range for petroleum fuels heat content, taken from the EIA-923 instructions.

Lower bound is for waste oil. Upper bound is for residual fuel oil.

https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

VALID_OIL_USD_PER_MMBTU = {
    "lower_bound": 5.0,
    "upper_bound": 33.0,
}
"""Historical petroleum price range from the EIA-923 Fuel Receipts and Costs table."""

##############################################################################
# String categorizations
##############################################################################

FUEL_CATEGORIES: dict[str, set[str]] = {
    "categories": {
        "coal": {
            "coal",
            "coal-subbit",
            "lignite",
            "coal(sb)",
            "coal (sb)",
            "coal-lignite",
            "coke",
            "coa",
            "lignite/coal",
            "coal - subbit",
            "coal-subb",
            "coal-sub",
            "coal-lig",
            "coal-sub bit",
            "coals",
            "ciak",
            "petcoke",
            "coal.oil",
            "coal/gas",
            "coal. gas",
            "coal & oil",
            "coal bit",
            "bit coal",
            "coal-unit #3",
            "coal-subbitum",
            "coal tons",
            "coal mcf",
            "coal unit #3",
            "pet. coke",
            "coal-u3",
            "coal&coke",
            "tons",
        },
        "oil": {
            "oil",
            "#6 oil",
            "#2 oil",
            "fuel oil",
            "jet",
            "no. 2 oil",
            "no.2 oil",
            "no.6& used",
            "used oil",
            "oil-2",
            "oil (#2)",
            "diesel oil",
            "residual oil",
            "# 2 oil",
            "resid. oil",
            "tall oil",
            "oil/gas",
            "no.6 oil",
            "oil-fuel",
            "oil-diesel",
            "oil / gas",
            "oil bbls",
            "oil bls",
            "no. 6 oil",
            "#1 kerosene",
            "diesel",
            "no. 2 oils",
            "blend oil",
            "#2oil diesel",
            "#2 oil-diesel",
            "# 2  oil",
            "light oil",
            "heavy oil",
            "gas.oil",
            "#2",
            "2",
            "6",
            "bbl",
            "no 2 oil",
            "no 6 oil",
            "#1 oil",
            "#6",
            "oil-kero",
            "oil bbl",
            "biofuel",
            "no 2",
            "kero",
            "#1 fuel oil",
            "no. 2  oil",
            "blended oil",
            "no 2. oil",
            "# 6 oil",
            "nno. 2 oil",
            "#2 fuel",
            "oill",
            "oils",
            "gas/oil",
            "no.2 oil gas",
            "#2 fuel oil",
            "oli",
            "oil (#6)",
            "oil/diesel",
            "2 oil",
            "#6 hvy oil",
            "jet fuel",
            "diesel/compos",
            "oil-8",
            "oil {6}",  # noqa: FS003
            "oil-unit #1",
            "bbl.",
            "oil.",
            "oil #6",
            "oil (6)",
            "oil(#2)",
            "oil-unit1&2",
            "oil-6",
            "#2 fue oil",
            "dielel oil",
            "dielsel oil",
            "#6 & used",
            "barrels",
            "oil un 1 & 2",
            "jet oil",
            "oil-u1&2",
            "oiul",
            "pil",
            "oil - 2",
            "#6 & used",
            "oial",
            "diesel fuel",
            "diesel/compo",
            "oil (used)",
        },
        "gas": {
            "gas",
            "gass",
            "methane",
            "natural gas",
            "blast gas",
            "gas mcf",
            "propane",
            "prop",
            "natural  gas",
            "nat.gas",
            "nat gas",
            "nat. gas",
            "natl gas",
            "ga",
            "gas`",
            "syngas",
            "ng",
            "mcf",
            "blast gaa",
            "nat  gas",
            "gac",
            "syngass",
            "prop.",
            "natural",
            "coal.gas",
            "n. gas",
            "lp gas",
            "natuaral gas",
            "coke gas",
            "gas #2016",
            "propane**",
            "* propane",
            "propane **",
            "gas expander",
            "gas ct",
            "# 6 gas",
            "#6 gas",
            "coke oven gas",
            "gas & oil",
            "gas/fuel oil",
        },
        "solar": {"solar"},
        "wind": {"wind"},
        "hydro": {"hydro"},
        "nuclear": {
            "nuclear",
            "grams of uran",
            "grams of",
            "grams of  ura",
            "grams",
            "nucleur",
            "nulear",
            "nucl",
            "nucleart",
            "nucelar",
            "gr.uranium",
            "grams of urm",
            "nuclear (9)",
            "nulcear",
            "nuc",
            "gr. uranium",
            "uranium",
            "nuclear mw da",
            "grams of ura",
            "nucvlear",
            "nuclear (1)",
        },
        "waste": {
            "waste",
            "tires",
            "tire",
            "refuse",
            "switchgrass",
            "wood waste",
            "woodchips",
            "biomass",
            "wood",
            "wood chips",
            "rdf",
            "tires/refuse",
            "tire refuse",
            "waste oil",
            "woodships",
            "tire chips",
            "tdf",
        },
        "other": {  # This should really be NA but we require a fuel_type_code_pudl
            "other",
            "na",
            "",
            "steam",
            "purch steam",
            "all",
            "n/a",
            "purch. steam",
            "composite",
            "composit",
            "mbtus",
            "total",
            "avg",
            "avg.",
            "blo",
            "all fuel",
            "comb.",
            "alt. fuels",
            "comb",
            "/#=2\x80â\x91?",
            "kã\xadgv¸\x9d?",
            "mbtu's",
            "gas, oil",
            "rrm",
            "3\x9c",
            "average",
            "furfural",
            "0",
            "watson bng",
            "toal",
            "bng",
            "# 6 & used",
            "combined",
            "blo bls",
            "compsite",
            "*",
            "compos.",
            "gas / oil",
            "mw days",
            "g",
            "c",
            "lime",
            "all fuels",
            "at right",
            "20",
            "1",
            "comp oil/gas",
            "all fuels to",
            "the right are",
            "c omposite",
            "all fuels are",
            "total pr crk",
            "all fuels =",
            "total pc",
            "comp",
            "alternative",
            "alt. fuel",
            "bio fuel",
            "total prairie",
            "kã\xadgv¸?",
            "m",
            "waste heat",
            "/#=2â?",
            "3",
            "—",
        },
    }
}
"""
A mapping a canonical fuel name to a set of strings which are used to represent that
fuel in the FERC Form 1 Reporting. Case is ignored, as all fuel strings are converted to
lower case in the data set.
"""

FUEL_UNIT_CATEGORIES: dict[str, set[str]] = {
    "categories": {
        "ton": {
            "ton",
            "toms",
            "taons",
            "tones",
            "col-tons",
            "toncoaleq",
            "coal",
            "tons coal eq",
            "coal-tons",
            "tons",
            "tons coal",
            "coal-ton",
            "tires-tons",
            "coal tons -2 ",
            "oil-tons",
            "coal tons 200",
            "ton-2000",
            "coal tons",
            "coal tons -2",
            "coal-tone",
            "tire-ton",
            "tire-tons",
            "ton coal eqv",
            "tos",
            "coal tons - 2",
            "c. t.",
            "c.t.",
            "t",
            "toncoalequiv",
        },
        "mcf": {
            "mcf",
            "mcf's",
            "mcfs",
            "mcf.",
            "mcfe",
            "gas mcf",
            '"gas" mcf',
            "gas-mcf",
            "mfc",
            "mct",
            " mcf",
            "msfs",
            "mlf",
            "mscf",
            "mci",
            "mcl",
            "mcg",
            "m.cu.ft.",
            "kcf",
            "(mcf)",
            "mcf *(4)",
            "mcf00",
            "m.cu.ft..",
            "1000 c.f",
        },
        "bbl": {
            "bbl",
            "barrel",
            "bbls",
            "barrels",
            "bbrl",
            "bbl.",
            "bbls.",
            "oil 42 gal",
            "oil-barrels",
            "barrrels",
            "bbl-42 gal",
            "oil-barrel",
            "bb.",
            "barrells",
            "bar",
            "bbld",
            "oil- barrel",
            "barrels    .",
            "bbl .",
            "barels",
            "barrell",
            "berrels",
            "bb",
            "bbl.s",
            "oil-bbl",
            "bls",
            "bbl:",
            "barrles",
            "blb",
            "propane-bbl",
            "barriel",
            "berriel",
            "barrile",
            "(bbl.)",
            "barrel *(4)",
            "(4) barrel",
            "bbf",
            "blb.",
            "(bbl)",
            "bb1",
            "bbsl",
            "barrrel",
            "barrels 100%",
            "bsrrels",
            "bbl's",
            "*barrels",
            "oil - barrels",
            "oil 42 gal ba",
            "bll",
            "boiler barrel",
            "gas barrel",
            '"boiler" barr',
            '"gas" barrel',
            '"boiler"barre',
            '"boiler barre',
            "barrels .",
            "bariel",
            "brrels",
            "oil barrel",
            "barreks",
            "oil-bbls",
            "oil-bbs",
            "boe",
        },
        "mmbbl": {"mmbbl", "mmbbls"},
        "gal": {"gal", "gallons", "gal.", "gals", "gals.", "gallon", "galllons"},
        "kgal": {
            "kgal",
            "oil(1000 gal)",
            "oil(1000)",
            "oil (1000)",
            "oil(1000",
            "oil(1000ga)",
            "1000 gals",
            "1000 gal",
        },
        "grams": {
            "gram",
            "grams",
            "gm u",
            "grams u235",
            "grams u-235",
            "grams of uran",
            "grams: u-235",
            "grams:u-235",
            "grams:u235",
            "grams u308",
            "grams: u235",
            "grams of",
            "grams - n/a",
            "gms uran",
            "s e uo2 grams",
            "gms uranium",
            "grams of urm",
            "gms. of uran",
            "grams (100%)",
            "grams v-235",
            "se uo2 grams",
            "grams u",
            "g",
            "grams of uranium",
        },
        "kg": {
            "kg",
            "kg of uranium",
            "kg uranium",
            "kilg. u-235",
            "kg u-235",
            "kilograms-u23",
            "kilograms u-2",
            "kilograms",
            "kg of",
            "kg-u-235",
            "kilgrams",
            "kilogr. u235",
            "uranium kg",
            "kg uranium25",
            "kilogr. u-235",
            "kg uranium 25",
            "kilgr. u-235",
            "kguranium 25",
            "kg-u235",
            "kgm",
        },
        "klbs": {
            "klbs",
            "k lbs.",
            "k lbs",
            "1000 / lbs",
            "1000 lbs",
        },
        "mmbtu": {
            "mmbtu",
            "mmbtus",
            "mbtus",
            "(mmbtu)",
            "mmbtu's",
            "nuclear-mmbtu",
            "nuclear-mmbt",
            "mmbtul",
        },
        "btu": {
            "btu",
            "btus",
        },
        "mwdth": {
            "mwdth",
            "mwd therman",
            "mw days-therm",
            "mwd thrml",
            "mwd thermal",
            "mwd/mtu",
            "mw days",
            "mwd",
            "mw day",
            "dth",
            "mwdaysthermal",
            "mw day therml",
            "mw days thrml",
            "nuclear mwd",
            "mmwd",
            "mw day/therml" "mw days/therm",
            "mw days (th",
            "ermal)",
        },
        "mwhth": {
            "nwh therm",
            "mwhth",
            "mwh them",
            "mwh threm",
            "mwh therm",
            "mwh",
            "mwh therms.",
            "mwh term.uts",
            "mwh thermal",
            "mwh thermals",
            "mw hr therm",
            "mwh therma",
            "mwh therm.uts",
        },
        "na_category": {
            "na_category",
            "na",
            "",
            "1265",
            "mwh units",
            "composite",
            "therms",
            "n/a",
            "mbtu/kg",
            "uranium 235",
            "oil",
            "ccf",
            "2261",
            "uo2",
            "(7)",
            "oil #2",
            "oil #6",
            '\x99å\x83\x90?"',
            "dekatherm",
            "0",
            "mw day/therml",
            "nuclear",
            "gas",
            "62,679",
            "mw days/therm",
            "uranium",
            "oil/gas",
            "thermal",
            "(thermal)",
            "se uo2",
            "181679",
            "83",
            "3070",
            "248",
            "273976",
            "747",
            "-",
            "are total",
            "pr. creek",
            "decatherms",
            "uramium",
            ".",
            "total pr crk",
            ">>>>>>>>",
            "all",
            "total",
            "alternative-t",
            "oil-mcf",
            "3303671",
            "929",
            "7182175",
            "319",
            "1490442",
            "10881",
            "1363663",
            "7171",
            "1726497",
            "4783",
            "7800",
            "12559",
            "2398",
            "creek fuels",
            "propane-barre",
            "509",
            "barrels/mcf",
            "propane-bar",
            "4853325",
            "4069628",
            "1431536",
            "708903",
            "mcf/oil (1000",
            "344",
            'å?"',
            "mcf / gallen",
            "none",
            "—",
        },
    }
}
"""
A mapping of canonical fuel units (keys) to sets of strings representing those
fuel units (values)
"""

PLANT_TYPE_CATEGORIES: dict[str, set[str]] = {
    "categories": {
        "steam": {
            "coal",
            "steam",
            "steam units 1 2 3",
            "steam units 4 5",
            "steam fossil",
            "steam turbine",
            "steam a",
            "steam 100",
            "steam units 1 2 3",
            "steams",
            "steam 1",
            "steam retired 2013",
            "stream",
            "steam units 1,2,3",
            "steam units 4&5",
            "steam units 4&6",
            "steam conventional",
            "unit total-steam",
            "unit total steam",
            "*resp. share steam",
            "resp. share steam",
            "steam (see note 1,",
            "steam (see note 3)",
            "mpc 50%share steam",
            "40% share steam" "steam (2)",
            "steam (3)",
            "steam (4)",
            "steam (5)",
            "steam (6)",
            "steam (7)",
            "steam (8)",
            "steam units 1 and 2",
            "steam units 3 and 4",
            "steam (note 1)",
            "steam (retired)",
            "steam (leased)",
            "coal-fired steam",
            "oil-fired steam",
            "steam/fossil",
            "steam (a,b)",
            "steam (a)",
            "stean",
            "steam-internal comb",
            "steam (see notes)",
            "steam units 4 & 6",
            "resp share stm note3",
            "mpc50% share steam",
            "mpc40%share steam",
            "steam - 64%",
            "steam - 100%",
            "steam (1) & (2)",
            "resp share st note3",
            "mpc 50% shares steam",
            "steam-64%",
            "steam-100%",
            "steam (see note 1)",
            "mpc 50% share steam",
            "steam units 1, 2, 3",
            "steam units 4, 5",
            "steam (2)",
            "steam (1)",
            "steam 4, 5",
            "steam - 72%",
            "steam (incl i.c.)",
            "steam- 72%",
            "steam;retired - 2013",
            "respondent's sh.-st.",
            "respondent's sh-st",
            "40% share steam",
            "resp share stm note3",
            "mpc50% share steam",
            "resp share st note 3",
            "\x02steam (1)",
            "coal fired steam tur",
            "coal fired steam turbine",
            "steam- 64%",
        },
        "combustion_turbine": {
            "combustion turbine",
            "gt",
            "gas turbine",
            "gas turbine # 1",
            "gas turbine",
            "gas turbine (note 1)",
            "gas turbines",
            "simple cycle",
            "combustion turbine",
            "comb.turb.peak.units",
            "gas turbine",
            "combustion turbine",
            "com turbine peaking",
            "gas turbine peaking",
            "comb turb peaking",
            "combustine turbine",
            "comb. turine",
            "conbustion turbine",
            "combustine turbine",
            "gas turbine (leased)",
            "combustion tubine",
            "gas turb",
            "gas turbine peaker",
            "gtg/gas",
            "simple cycle turbine",
            "gas-turbine",
            "gas turbine-simple",
            "gas turbine - note 1",
            "gas turbine #1",
            "simple cycle",
            "gasturbine",
            "combustionturbine",
            "gas turbine (2)",
            "comb turb peak units",
            "jet engine",
            "jet powered turbine",
            "*gas turbine",
            "gas turb.(see note5)",
            "gas turb. (see note",
            "combutsion turbine",
            "combustion turbin",
            "gas turbine-unit 2",
            "gas - turbine",
            "comb turbine peaking",
            "gas expander turbine",
            "jet turbine",
            "gas turbin (lease",
            "gas turbine (leased",
            "gas turbine/int. cm",
            "comb.turb-gas oper.",
            "comb.turb.gas/oil op",
            "comb.turb.oil oper.",
            "jet",
            "comb. turbine (a)",
            "gas turb.(see notes)",
            "gas turb(see notes)",
            "comb. turb-gas oper",
            "comb.turb.oil oper",
            "gas turbin (leasd)",
            "gas turbne/int comb",
            "gas turbine (note1)",
            "combution turbin",
            "* gas turbine",
            "add to gas turbine",
            "gas turbine (a)",
            "gas turbinint comb",
            "gas turbine (note 3)",
            "resp share gas note3",
            "gas trubine",
            "*gas turbine(note3)",
            "gas turbine note 3,6",
            "gas turbine note 4,6",
            "gas turbine peakload",
            "combusition turbine",
            "gas turbine (lease)",
            "comb. turb-gas oper.",
            "combution turbine",
            "combusion turbine",
            "comb. turb. oil oper",
            "combustion burbine",
            "combustion and gas",
            "comb. turb.",
            "gas turbine (lease",
            "gas turbine (leasd)",
            "gas turbine/int comb",
            "*gas turbine(note 3)",
            "gas turbine (see nos",
            "i.c.e./gas turbine",
            "gas turbine/intcomb",
            "cumbustion turbine",
            "gas turb, int. comb.",
            "gas turb, diesel",
            "gas turb, int. comb",
            "i.c.e/gas turbine",
            # "diesel turbine", # this was in both this CT category and IC...
            "comubstion turbine",
            "i.c.e. /gas turbine",
            "i.c.e/ gas turbine",
            "i.c.e./gas tubine",
            "gas turbine; retired",
        },
        "combined_cycle": {
            "Combined cycle",
            "combined cycle",
            "combined",
            "gas & steam turbine",
            "gas turb. & heat rec",
            "combined cycle",
            "com. cyc",
            "com. cycle",
            "gas turb-combined cy",
            "combined cycle ctg",
            "combined cycle - 40%",
            "com cycle gas turb",
            "combined cycle oper",
            "gas turb/comb. cyc",
            "combine cycle",
            "cc",
            "comb. cycle",
            "gas turb-combined cy",
            "steam and cc",
            "steam cc",
            "gas steam",
            "ctg steam gas",
            "steam comb cycle",
            "gas/steam comb. cycl",
            "gas/steam",
            "gas turbine-combined cycle",
            "steam (comb. cycle)" "gas turbine/steam",
            "steam & gas turbine",
            "gas trb & heat rec",
            "steam & combined ce",
            "st/gas turb comb cyc",
            "gas tur & comb cycl",
            "combined cycle (a,b)",
            "gas turbine/ steam",
            "steam/gas turb.",
            "steam & comb cycle",
            "gas/steam comb cycle",
            "comb cycle (a,b)",
            "igcc",
            "steam/gas turbine",
            "gas turbine / steam",
            "gas tur & comb cyc",
            "comb cyc (a) (b)",
            "comb cycle",
            "comb cyc",
            "combined turbine",
            "combine cycle oper",
            "comb cycle/steam tur",
            "cc / gas turb",
            "steam (comb. cycle)",
            "steam & cc",
            "gas turbine/steam",
            "gas turb/cumbus cycl",
            "gas turb/comb cycle",
            "gasturb/comb cycle",
            "gas turb/cumb. cyc",
            "igcc/gas turbine",
            "gas / steam",
            "ctg/steam-gas",
            "ctg/steam -gas",
            "gas fired cc turbine",
            "combinedcycle",
            "comb cycle gas turb",
            "combined cycle opern",
            "comb. cycle gas turb",
            "ngcc",
        },
        "nuclear": {
            "nuclear",
            "nuclear (3)",
            "steam(nuclear)",
            "nuclear(see note4)" "nuclear steam",
            "nuclear turbine",
            "nuclear - steam",
            "nuclear (a)(b)(c)",
            "nuclear (b)(c)",
            "* nuclear",
            "nuclear (b) (c)",
            "nuclear (see notes)",
            "steam (nuclear)",
            "* nuclear (note 2)",
            "nuclear (note 2)",
            "nuclear (see note 2)",
            "nuclear(see note4)",
            "nuclear steam",
            "nuclear(see notes)",
            "nuclear-steam",
            "nuclear (see note 3)",
        },
        "geothermal": {"steam - geothermal", "steam_geothermal", "geothermal"},
        "internal_combustion": {
            "ic",
            "internal combustion",
            "internal comb.",
            "internl combustion",
            "diesel turbine",
            "int combust (note 1)",
            "int. combust (note1)",
            "int.combustine",
            "comb. cyc",
            "internal comb",
            "diesel",
            "diesel engine",
            "internal combustion",
            "int combust - note 1",
            "int. combust - note1",
            "internal comb recip",
            "internal combustion reciprocating",
            "reciprocating engine",
            "comb. turbine",
            "internal combust.",
            "int. combustion (1)",
            "*int combustion (1)",
            "*internal combust'n",
            "internal",
            "internal comb.",
            "steam internal comb",
            "combustion",
            "int. combustion",
            "int combust (note1)",
            "int. combustine",
            "internl combustion",
            "*int. combustion (1)",
            "internal conbustion",
        },
        "wind": {
            "wind",
            "wind energy",
            "wind turbine",
            "wind - turbine",
            "wind generation",
            "wind turbin",
        },
        "photovoltaic": {
            "solar photovoltaic",
            "photovoltaic",
            "solar",
            "solar project",
        },
        "solar_thermal": {"solar thermal"},
        "na_category": {
            "na_category",
            "na",
            "",
            "n/a",
            "see pgs 402.1-402.3",
            "see pgs 403.1-403.9",
            "respondent's share",
            "--",
            "—",
            "footnote",
            "(see note 7)",
            "other",
            "not applicable",
            "peach bottom",
            "none.",
            "fuel facilities",
            "0",
            "not in service",
            "none",
            "common expenses",
            "expenses common to",
            "retired in 1981",
            "retired in 1978",
            "unit total (note3)",
            "unit total (note2)",
            "resp. share (note2)",
            "resp. share (note8)",
            "resp. share (note 9)",
            "resp. share (note11)",
            "resp. share (note4)",
            "resp. share (note6)",
            "conventional",
            "expenses commom to",
            "not in service in",
            "unit total (note 3)",
            "unit total (note 2)",
            "resp. share (note 8)",
            "resp. share (note 3)",
            "resp. share note 11",
            "resp. share (note 4)",
            "resp. share (note 6)",
            "(see note 5)",
            "resp. share (note 2)",
            "package",
            "(left blank)",
            "common",
            "0.0000",
            "other generation",
            "resp share (note 11)",
            "retired",
            "storage/pipelines",
            "sold april 16, 1999",
            "sold may 07, 1999",
            "plants sold in 1999",
            "gas",
            "not applicable.",
            "resp. share - note 2",
            "resp. share - note 8",
            "resp. share - note 9",
            "resp share - note 11",
            "resp. share - note 4",
            "resp. share - note 6",
            "plant retired- 2013",
            "retired - 2013",
            "resp share - note 5",
            "resp. share - note 7",
            "non-applicable",
            "other generation plt",
            "combined heat/power",
            "oil",
            "fuel oil",
        },
    }
}
"""
A mapping from canonical plant kinds (keys) to the associated freeform strings (values)
identified as being associated with that kind of plant in the FERC Form 1 raw data.
There are many strings that weren't categorized, Solar and Solar Project were not
classified as these do not indicate if they are solar thermal or photovoltaic. Variants
on Steam (e.g. "steam 72" and "steam and gas") were classified based on additional
research of the plants on the Internet.
"""

CONSTRUCTION_TYPE_CATEGORIES: dict[str, set[str]] = {
    "categories": {
        "outdoor": {
            "outdoor",
            "outdoor boiler",
            "full outdoor",
            "outdoor boiler",
            "outdoor boilers",
            "outboilers",
            "fuel outdoor",
            "full outdoor",
            "outdoors",
            "outdoor",
            "boiler outdoor& full",
            "boiler outdoor&full",
            "outdoor boiler& full",
            "full -outdoor",
            "outdoor steam",
            "outdoor boiler",
            "ob",
            "outdoor automatic",
            "outdoor repower",
            "full outdoor boiler",
            "fo",
            "outdoor boiler & ful",
            "full-outdoor",
            "fuel outdoor",
            "outoor",
            "outdoor",
            "outdoor  boiler&full",
            "boiler outdoor &full",
            "outdoor boiler &full",
            "boiler outdoor & ful",
            "outdoor-boiler",
            "outdoor - boiler",
            "outdoor const.",
            "4 outdoor boilers",
            "3 outdoor boilers",
            "full outdoor",
            "full outdoors",
            "full oudoors",
            "outdoor (auto oper)",
            "outside boiler",
            "outdoor boiler&full",
            "outdoor hrsg",
            "outdoor hrsg",
            "outdoor-steel encl.",
            "boiler-outdr & full",
            "con.& full outdoor",
            "partial outdoor",
            "outdoor (auto. oper)",
            "outdoor (auto.oper)",
            "outdoor construction",
            "1 outdoor boiler",
            "2 outdoor boilers",
            "outdoor enclosure",
            "2 outoor boilers",
            "boiler outdr.& full",
            "boiler outdr. & full",
            "ful outdoor",
            "outdoor-steel enclos",
            "outdoor (auto oper.)",
            "con. & full outdoor",
            "outdore",
            "boiler & full outdor",
            "full & outdr boilers",
            "outodoor (auto oper)",
            "outdoor steel encl.",
            "full outoor",
            "boiler & outdoor ful",
            "otdr. blr. & f. otdr",
            "f.otdr & otdr.blr.",
            "oudoor (auto oper)",
            "outdoor constructin",
            "f. otdr. & otdr. blr",
            "outdoor boiler & fue",
            "outdoor boiler &fuel",
        },
        "semioutdoor": {
            "semioutdoor" "more than 50% outdoors",
            "more than 50% outdoo",
            "more than 50% outdos",
            "over 50% outdoor",
            "over 50% outdoors",
            "semi-outdoor",
            "semi - outdoor",
            "semi outdoor",
            "semi-enclosed",
            "semi-outdoor boiler",
            "semi outdoor boiler",
            "semi- outdoor",
            "semi - outdoors",
            "semi -outdoor" "conven & semi-outdr",
            "conv & semi-outdoor",
            "conv & semi- outdoor",
            "convent. semi-outdr",
            "conv. semi outdoor",
            "conv(u1)/semiod(u2)",
            "conv u1/semi-od u2",
            "conv-one blr-semi-od",
            "convent semioutdoor",
            "conv. u1/semi-od u2",
            "conv - 1 blr semi od",
            "conv. ui/semi-od u2",
            "conv-1 blr semi-od",
            "conven. semi-outdoor",
            "conv semi-outdoor",
            "u1-conv./u2-semi-od",
            "u1-conv./u2-semi -od",
            "convent. semi-outdoo",
            "u1-conv. / u2-semi",
            "conven & semi-outdr",
            "semi -outdoor",
            "outdr & conventnl",
            "conven. full outdoor",
            "conv. & outdoor blr",
            "conv. & outdoor blr.",
            "conv. & outdoor boil",
            "conv. & outdr boiler",
            "conv. & out. boiler",
            "convntl,outdoor blr",
            "outdoor & conv.",
            "2 conv., 1 out. boil",
            "outdoor/conventional",
            "conv. boiler outdoor",
            "conv-one boiler-outd",
            "conventional outdoor",
            "conventional outdor",
            "conv. outdoor boiler",
            "conv.outdoor boiler",
            "conventional outdr.",
            "conven,outdoorboiler",
            "conven full outdoor",
            "conven,full outdoor",
            "1 out boil, 2 conv",
            "conv. & full outdoor",
            "conv. & outdr. boilr",
            "conv outdoor boiler",
            "convention. outdoor",
            "conv. sem. outdoor",
            "convntl, outdoor blr",
            "conv & outdoor boil",
            "conv & outdoor boil.",
            "outdoor & conv",
            "conv. broiler outdor",
            "1 out boilr, 2 conv",
            "conv.& outdoor boil.",
            "conven,outdr.boiler",
            "conven,outdr boiler",
            "outdoor & conventil",
            "1 out boilr 2 conv",
            "conv & outdr. boilr",
            "conven, full outdoor",
            "conven full outdr.",
            "conven, full outdr.",
            "conv/outdoor boiler",
            "convnt'l outdr boilr",
            "1 out boil 2 conv",
            "conv full outdoor",
            "conven, outdr boiler",
            "conventional/outdoor",
            "conv&outdoor boiler",
            "outdoor & convention",
            "conv & outdoor boilr",
            "conv & full outdoor",
            "convntl. outdoor blr",
            "conv - ob",
            "1conv'l/2odboilers",
            "2conv'l/1odboiler",
            "conv-ob",
            "conv.-ob",
            "1 conv/ 2odboilers",
            "2 conv /1 odboilers",
            "conv- ob",
            "conv -ob",
            "con sem outdoor",
            "cnvntl, outdr, boilr",
            "less than 50% outdoo",
            "less than 50% outdoors",
            "under 50% outdoor",
            "under 50% outdoors",
            "1cnvntnl/2odboilers",
            "2cnvntnl1/1odboiler",
            "con & ob",
            "combination (b)",
            "indoor & outdoor",
            "conven. blr. & full",
            "conv. & otdr. blr.",
            "combination",
            "indoor and outdoor",
            "conven boiler & full",
            "2conv'l/10dboiler",
            "4 indor/outdr boiler",
            "4 indr/outdr boilerr",
            "4 indr/outdr boiler",
            "indoor & outdoof",
        },
        "conventional": {
            "conventional",
            "conventional",
            "conventional boiler",
            "conventional - boiler",
            "conv-b",
            "conventionall",
            "convention",
            "conventional",
            "coventional",
            "conven full boiler",
            "c0nventional",
            "conventtional",
            "convential" "underground",
            "conventional bulb",
            "conventrional",
            "*conventional",
            "convential",
            "convetional",
            "conventioanl",
            "conventioinal",
            "conventaional",
            "indoor construction",
            "convenional",
            "conventional steam",
            "conventinal",
            "convntional",
            "conventionl",
            "conventionsl",
            "conventiional",
            "convntl steam plants",
            "indoor const.",
            "full indoor",
            "indoor",
            "indoor automatic",
            "indoor boiler",
            "indoor boiler and steam turbine",
            "(peak load) indoor",
            "conventionl,indoor",
            "conventionl, indoor",
            "conventional, indoor",
            "conventional;outdoor",
            "conven./outdoor",
            "conventional;semi-ou",
            "comb. cycle indoor",
            "comb cycle indoor",
            "3 indoor boiler",
            "2 indoor boilers",
            "1 indoor boiler",
            "2 indoor boiler",
            "3 indoor boilers",
            "fully contained",
            "conv - b",
            "conventional/boiler",
            "cnventional",
            "comb. cycle indooor",
            "sonventional",
            "ind enclosures",
            "conentional",
            "conventional - boilr",
            "indoor boiler and st",
        },
        "na_category": {
            "na_category",
            "na",
            "",
            "automatic operation",
            "comb. turb. installn",
            "comb. turb. instaln",
            "com. turb. installn",
            "n/a",
            "for detailed info.",
            "for detailed info",
            "combined cycle",
            "not applicable",
            "gas",
            "heated individually",
            "metal enclosure",
            "pressurized water",
            "nuclear",
            "jet engine",
            "gas turbine",
            "storage/pipelines",
            "0",
            "during 1994",
            "peaking - automatic",
            "gas turbine/int. cm",
            "2 oil/gas turbines",
            "wind",
            "package",
            "mobile",
            "auto-operated",
            "steam plants",
            "other production",
            "all nuclear plants",
            "other power gen.",
            "automatically operad",
            "automatically operd",
            "circ fluidized bed",
            "jet turbine",
            "gas turbne/int comb",
            "automatically oper.",
            "retired 1/1/95",
            "during 1995",
            "1996. plant sold",
            "reactivated 7/1/96",
            "gas turbine/int comb",
            "portable",
            "head individually",
            "automatic opertion",
            "peaking-automatic",
            "cycle",
            "full order",
            "circ. fluidized bed",
            "gas turbine/intcomb",
            "0.0000",
            "none",
            "2 oil / gas",
            "block & steel",
            "and 2000",
            "comb.turb. instaln",
            "automatic oper.",
            "pakage",
            "---",
            "—",
            "n/a (ct)",
            "comb turb instain",
            "ind encloures",
            "2 oil /gas turbines",
            "combustion turbine",
            "1970",
            "gas/oil turbines",
            "combined cycle steam",
            "pwr",
            "2 oil/ gas",
            "2 oil / gas turbines",
            "gas / oil turbines",
            "no boiler",
            "internal combustion",
            "gasturbine no boiler",
            "boiler",
            "tower -10 unit facy",
            "gas trubine",
            "4 gas/oil trubines",
            "2 oil/ 4 gas/oil tur",
            "5 gas/oil turbines",
            "tower 16",
            "2 on 1 gas turbine",
            "tower 23",
            "tower -10 unit",
            "tower - 101 unit",
            "3 on 1 gas turbine",
            "tower - 10 units",
            "tower - 165 units",
            "wind turbine",
            "fixed tilt pv",
            "tracking pv",
            "o",
            "wind trubine",
            "wind generator",
            "subcritical",
            "sucritical",
            "simple cycle",
            "simple & reciprocat",
            "solar",
            "pre-fab power plant",
            "prefab power plant",
            "prefab. power plant",
            "pump storage",
            "underground",
            "see page 402",
            "conv. underground",
            "conven. underground",
            "conventional (a)",
            "non-applicable",
            "duct burner",
            "see footnote",
            "simple and reciprocat",
        },
    }
}
"""
A dictionary of construction types (keys) and lists of construction type strings
associated with each type (values) from FERC Form 1.

There are many strings that weren't categorized, including crosses between conventional
and outdoor, PV, wind, combined cycle, and internal combustion. The lists are broken out
into the two types specified in Form 1: conventional and outdoor. These lists are
inclusive so that variants of conventional (e.g. "conventional full") and outdoor (e.g.
"outdoor full" and "outdoor hrsg") are included.
"""

##############################################################################
# Fully assembled set of FERC 1 transformation parameters
##############################################################################

TRANSFORM_PARAMS = {
    "fuel_ferc1": {
        "rename_columns": {
            "dbf": {
                "columns": {
                    "respondent_id": "utility_id_ferc1",
                    "plant_name": "plant_name_ferc1",
                    "fuel": "fuel_type_code_pudl",
                    "fuel_unit": "fuel_units",
                    # Original fuel heat content is reported...:
                    # * coal: almost entirely BTU per POUND
                    # * gas: ~half MMBTU per cubic foot, ~half MMBTU per Mcf
                    # * oil: almost entirely BTU per gallon
                    "fuel_avg_heat": "fuel_btu_per_unit",
                    "fuel_quantity": "fuel_consumed_units",
                    "fuel_cost_burned": "fuel_cost_per_unit_burned",
                    "fuel_cost_delvd": "fuel_cost_per_unit_delivered",
                    # Note: Original fuel_cost_btu is misleadingly named
                    "fuel_cost_btu": "fuel_cost_per_mmbtu",
                    "fuel_generaton": "fuel_btu_per_kwh",
                    "report_prd": "report_prd",
                    "row_prvlg": "row_prvlg",
                    "report_year": "report_year",
                    "row_number": "row_number",
                    "fuel_cost_kwh": "fuel_cost_per_kwh",
                    "spplmnt_num": "spplmnt_num",
                    "row_seq": "row_seq",
                }
            },
            "xbrl": {
                "columns": {
                    "PlantNameAxis": "plant_name_ferc1",
                    "FuelKindAxis": "fuel_type_code_pudl",
                    "FuelUnit": "fuel_units",
                    "FuelBurnedAverageHeatContent": "fuel_btu_per_unit",
                    "QuantityOfFuelBurned": "fuel_consumed_units",
                    "AverageCostOfFuelPerUnitBurned": "fuel_cost_per_unit_burned",
                    "AverageCostOfFuelPerUnitAsDelivered": "fuel_cost_per_unit_delivered",
                    "AverageCostOfFuelBurnedPerMillionBritishThermalUnit": "fuel_cost_per_mmbtu",
                    "AverageBritishThermalUnitPerKilowattHourNetGeneration": "fuel_btu_per_kwh",
                    "AverageCostOfFuelBurnedPerKilowattHourNetGeneration": "fuel_cost_per_kwh",
                    "ReportYear": "report_year",
                    "FuelKind": "fuel_kind",
                    "end_date": "end_date",
                    "entity_id": "entity_id",
                    "start_date": "start_date",
                }
            },
        },
        "categorize_strings": {
            "fuel_type_code_pudl": FUEL_CATEGORIES,
            "fuel_units": FUEL_UNIT_CATEGORIES,
        },
        "convert_units": {
            "fuel_btu_per_unit": BTU_TO_MMBTU,
            "fuel_btu_per_kwh": BTU_PERKWH_TO_MMBTU_PERMWH,
            "fuel_cost_per_kwh": PERKWH_TO_PERMWH,
        },
        "normalize_strings": {
            "plant_name_ferc1": True,
            "fuel_type_code_pudl": True,
            "fuel_units": True,
        },
        "correct_units": [
            {
                "col": "fuel_mmbtu_per_unit",
                "query": "fuel_type_code_pudl=='coal'",
                "valid_range": VALID_COAL_MMBTU_PER_TON,
                "unit_conversions": [
                    PERPOUND_TO_PERSHORTTON,
                    BTU_TO_MMBTU,
                ],
            },
            {
                "col": "fuel_cost_per_mmbtu",
                "query": "fuel_type_code_pudl=='coal'",
                "valid_range": VALID_COAL_USD_PER_MMBTU,
                "unit_conversions": [
                    CENTS_TO_DOLLARS,
                ],
            },
            {
                "col": "fuel_mmbtu_per_unit",
                "query": "fuel_type_code_pudl=='gas'",
                "valid_range": VALID_GAS_MMBTU_PER_MCF,
                "unit_conversions": [
                    PERCF_TO_PERMCF,
                    BTU_TO_MMBTU,
                ],
            },
            {
                "col": "fuel_cost_per_mmbtu",
                "query": "fuel_type_code_pudl=='gas'",
                "valid_range": VALID_GAS_USD_PER_MMBTU,
                "unit_conversions": [
                    CENTS_TO_DOLLARS,
                ],
            },
            {
                "col": "fuel_mmbtu_per_unit",
                "query": "fuel_type_code_pudl=='oil'",
                "valid_range": VALID_OIL_MMBTU_PER_BBL,
                "unit_conversions": [
                    PERGALLON_TO_PERBARREL,
                    BTU_TO_MMBTU,  # Why was this omitted in the old corrections?
                ],
            },
            {
                "col": "fuel_cost_per_mmbtu",
                "query": "fuel_type_code_pudl=='oil'",
                "valid_range": VALID_OIL_USD_PER_MMBTU,
                "unit_conversions": [
                    CENTS_TO_DOLLARS,
                ],
            },
        ],
    },
    "plants_steam_ferc1": {
        "normalize_strings": {
            "plant_name_ferc1": True,
            "construction_type": True,
            "plant_type": True,
        },
        "nullify_outliers": {
            "construction_year": VALID_PLANT_YEARS,
            "installation_year": VALID_PLANT_YEARS,
        },
        "categorize_strings": {
            "construction_type": CONSTRUCTION_TYPE_CATEGORIES,
            "plant_type": PLANT_TYPE_CATEGORIES,
        },
        "convert_units": {
            "capex_per_kw": PERKW_TO_PERMW,
            "opex_per_kwh": PERKWH_TO_PERMWH,
            "net_generation_kwh": KWH_TO_MWH,
        },
        "rename_columns": {
            "dbf": {
                "columns": {
                    "cost_structure": "capex_structures",
                    "expns_misc_power": "opex_misc_power",
                    "plant_name": "plant_name_ferc1",
                    "plnt_capability": "plant_capability_mw",
                    "expns_plants": "opex_plants",
                    "expns_misc_steam": "opex_misc_steam",
                    "cost_per_kw": "capex_per_kw",
                    "when_not_limited": "not_water_limited_capacity_mw",
                    "asset_retire_cost": "asset_retirement_cost",
                    "expns_steam_othr": "opex_steam_other",
                    "expns_transfer": "opex_transfer",
                    "expns_engnr": "opex_engineering",
                    "avg_num_of_emp": "avg_num_employees",
                    "cost_of_plant_to": "capex_total",
                    "expns_rents": "opex_rents",
                    "tot_prdctn_expns": "opex_production_total",
                    "plant_kind": "plant_type",
                    "respondent_id": "utility_id_ferc1",
                    "expns_operations": "opex_operations",
                    "cost_equipment": "capex_equipment",
                    "type_const": "construction_type",
                    "plant_hours": "plant_hours_connected_while_generating",
                    "expns_coolants": "opex_coolants",
                    "expns_fuel": "opex_fuel",
                    "when_limited": "water_limited_capacity_mw",
                    "expns_kwh": "opex_per_kwh",
                    "expns_allowances": "opex_allowances",
                    "expns_steam": "opex_steam",
                    "yr_const": "construction_year",
                    "yr_installed": "installation_year",
                    "expns_boiler": "opex_boiler",
                    "peak_demand": "peak_demand_mw",
                    "cost_land": "capex_land",
                    "tot_capacity": "capacity_mw",
                    "net_generation": "net_generation_kwh",
                    "expns_electric": "opex_electric",
                    "expns_structures": "opex_structures",
                    "report_year": "report_year",
                    "report_prd": "report_prd",
                    "row_prvlg": "row_prvlg",
                    "row_number": "row_number",
                    "spplmnt_num": "spplmnt_num",
                    "row_seq": "row_seq",
                }
            },
            "xbrl": {
                "columns": {
                    "CostOfStructuresAndImprovementsSteamProduction": "capex_structures",
                    "MiscellaneousSteamPowerExpenses": "opex_misc_power",
                    "PlantNameAxis": "plant_name_ferc1",
                    "NetContinuousPlantCapability": "plant_capability_mw",
                    "MaintenanceOfElectricPlantSteamPowerGeneration": "opex_plants",
                    "MaintenanceOfMiscellaneousSteamPlant": "opex_misc_steam",
                    "CostPerKilowattOfInstalledCapacity": "capex_per_kw",
                    "NetContinuousPlantCapabilityNotLimitedByCondenserWater": "not_water_limited_capacity_mw",
                    "AssetRetirementCostsSteamProduction": "asset_retirement_cost",
                    "SteamFromOtherSources": "opex_steam_other",
                    "SteamTransferredCredit": "opex_transfer",
                    "MaintenanceSupervisionAndEngineeringSteamPowerGeneration": "opex_engineering",
                    "PlantAverageNumberOfEmployees": "avg_num_employees",
                    "CostOfPlant": "capex_total",
                    "RentsSteamPowerGeneration": "opex_rents",
                    "PowerProductionExpensesSteamPower": "opex_production_total",
                    "PlantKind": "plant_type",
                    "OperationSupervisionAndEngineeringExpense": "opex_operations",
                    "CostOfEquipmentSteamProduction": "capex_equipment",
                    "PlantConstructionType": "construction_type",
                    "PlantHoursConnectedToLoad": "plant_hours_connected_while_generating",
                    "CoolantsAndWater": "opex_coolants",
                    "FuelSteamPowerGeneration": "opex_fuel",
                    "NetContinuousPlantCapabilityLimitedByCondenserWater": "water_limited_capacity_mw",
                    "ExpensesPerNetKilowattHour": "opex_per_kwh",
                    "Allowances": "opex_allowances",
                    "SteamExpensesSteamPowerGeneration": "opex_steam",
                    "YearPlantOriginallyConstructed": "construction_year",
                    "YearLastUnitOfPlantInstalled": "installation_year",
                    "MaintenanceOfBoilerPlantSteamPowerGeneration": "opex_boiler",
                    "NetPeakDemandOnPlant": "peak_demand_mw",
                    "CostOfLandAndLandRightsSteamProduction": "capex_land",
                    "InstalledCapacityOfPlant": "capacity_mw",
                    "NetGenerationExcludingPlantUse": "net_generation_kwh",
                    "ElectricExpensesSteamPowerGeneration": "opex_electric",
                    "MaintenanceOfStructuresSteamPowerGeneration": "opex_structures",
                    "ReportYear": "report_year",
                    "entity_id": "entity_id",
                }
            },
        },
    },
}
"""
A dictionary of table transform parameters.

``rename_columns`` must to include ``entity_id`` even though we are not actually
renaming it because it is used as a PK.

TODO: Add more complete docs in there...
"""


################################################################################
# Transformation Parameter Models
################################################################################
@enum.unique
class Ferc1Source(enum.Enum):
    """Enumeration of allowed FERC 1 raw data sources."""

    XBRL = "xbrl"
    DBF = "dbf"


@enum.unique
class Ferc1TableId(enum.Enum):
    """Enumeration of the allowable FERC 1 table IDs.

    Hard coding this seems bad. Somehow it should be either defined in the context of
    the Package, the Ferc1Settings, an etl_group, or DataSource. All of the table
    transformers associated with a given data source should have a table_id that's
    from that data source's subset of the database. Where should this really happen?

    Alternatively, the allowable values could be derived *from* the structure of the
    Package.

    """

    FUEL_FERC1 = "fuel_ferc1"
    PLANTS_STEAM_FERC1 = "plants_steam_ferc1"
    PLANTS_HYDRO_FERC1 = "plants_hydro_ferc1"
    PLANTS_SMALL_FERC1 = "plants_small_ferc1"
    PLANTS_PUMPED_STORAGE_FERC1 = "plants_pumped_storage_ferc1"
    PLANT_IN_SERVICE_FERC1 = "plant_in_service_ferc1"
    PURCHASED_POWER = "purchased_power_ferc1"


class TransformParams(BaseModel):
    """An immutable base model for transformation parameters."""

    class Config:
        """Prevent parameters from changing part way through."""

        allow_mutation = False


class MultiColumnTransformParams(TransformParams):
    """Transform params that apply to several columns in a table.

    The keys are column names, and the values must all be the same type of
    :class:`TransformParams` object, since MultiColumnTransformParams are used by
    :class:`MultiColumnTransformFn` callables.

    Individual subclasses are dynamically generated for each multi-column transformation
    specified within a :class:`TableTransformParams` object.

    """

    @root_validator
    def single_param_type(cls, params):  # noqa: N805
        """Check that all TransformParams in the dictionary are of the same type."""
        param_types = {type(params[col]) for col in params}
        if len(param_types) > 1:
            raise ValueError(
                "Found multiple parameter types in multi-column transform params: "
                f"{param_types}"
            )
        return params


class RenameColumns(TransformParams):
    """A dictionary for mapping old column names to new column names in a dataframe."""

    columns: dict[str, str] = {}


class Ferc1RenameColumns(TransformParams):
    """Dictionaries for renaming either XBRL or DBF derived FERC 1 columns.

    This is FERC 1 specific, because we need to store both DBF and XBRL rename
    dictionaires separately.

    Potential validations:

    * Validate that all keys appear in the original dbf/xbrl sources.
      This has to be true, but right now we don't have stored metadata enumerating all
      of the columns that exist in the raw data, so we don't have anything to check
      against. Implement once when we have schemas defined for after the extract step.

    * Validate all values appear in PUDL tables, and all expected PUDL names are mapped.
      Actually we can't require that the rename values appear in the PUDL tables,
      because there will be cases in which the original column gets dropped or modified,
      e.g. in the case of unit conversions with a column rename.

    """

    dbf: RenameColumns = {}
    xbrl: RenameColumns = {}


class StringCategories(TransformParams):
    """Defines mappings to clean up manually categorized freeform strings.

    Each key in a stringmap is a cleaned output category, and each value is the set of
    all strings which should be replaced with associated clean output category.

    """

    categories: dict[str, set[str]]
    na_category: str = "na_category"

    @validator("categories")
    def categories_are_disjoint(cls, v):
        """Ensure that each string to be categorized only appears in one category."""
        for cat1, cat2 in combinations(v, 2):
            intersection = set(v[cat1]).intersection(v[cat2])
            if intersection:
                raise ValueError(
                    f"String categories are not disjoint. {cat1} and {cat2} both "
                    f"contain these values: {intersection}"
                )
        return v

    @validator("categories")
    def categories_are_idempotent(cls, v):
        """Ensure that every category contains the string it will map to.

        This ensures that if the categorization is applied more than once, it doesn't
        change the output.
        """
        for cat in v:
            if cat not in v[cat]:
                logger.info(f"String category {cat} does not map to itself. Adding it.")
                v[cat] = v[cat].union({cat})
        return v

    @property
    def mapping(self) -> dict[str, str]:
        """A 1-to-1 mapping appropriate for use with :meth:`pd.Series.map`."""
        return {
            string: cat for cat in self.categories for string in self.categories[cat]
        }


class UnitConversion(TransformParams):
    """A column-wise unit conversion.

    The default values will result in no alteration of the column.
    """

    multiplier: float = 1.0  # By default, multiply by 1 (no change)
    adder: float = 0.0  # By default, add 0 (no change)
    pattern: typing.Pattern = r"^(.*)$"  # By default, match the whole column namme
    repl: str = r"\1"  # By default, replace the whole column name with itself.


class ValidRange(TransformParams):
    """Column level specification of min and/or max values."""

    lower_bound: float = -np.inf
    upper_bound: float = np.inf

    @validator("upper_bound")
    def upper_bound_gte_lower_bound(cls, v, values, **kwargs):
        """Require upper bound to be greater than or equal to lower bound."""
        if values["lower_bound"] > v:
            raise ValueError("upper_bound must be greater than or equal to lower_bound")
        return v


class UnitCorrections(TransformParams):
    """Fix outlying values resulting from unit errors by muliplying by a constant.

    Note that since the unit correction depends on other columns in the dataframe to
    select a relevant subset of records, it is a table transform not a column transform,
    and so needs to know what column it applies to internally.

    """

    col: str
    query: str
    valid_range: ValidRange
    unit_conversions: list[UnitConversion]

    @validator("unit_conversions")
    def no_column_rename(cls, v):
        """Require that all unit conversions result in no column renaming.

        This constraint is imposed so that the same unit conversion definitions
        can be re-used both for unit corrections and columnwise unit conversions.
        """
        new_conversions = []
        for conv in v:
            new_conversions.append(
                UnitConversion(multiplier=conv.multiplier, adder=conv.adder)
            )
        return new_conversions


class TableTransformParams(TransformParams):
    """All defined transformation parameters for a table."""

    class Config:
        """Only allow the known table transform params."""

        extra = "forbid"

    rename_columns: Ferc1RenameColumns
    convert_units: dict[str, UnitConversion] = {}
    categorize_strings: dict[str, StringCategories] = {}
    nullify_outliers: dict[str, ValidRange] = {}
    normalize_strings: dict[str, bool] = {}
    correct_units: list[UnitCorrections] = []

    @classmethod
    def from_id(cls, table_id: Ferc1TableId) -> "TableTransformParams":
        """A factory method that looks up transform parameters based on table_id."""
        return cls(**TRANSFORM_PARAMS[table_id.value])


################################################################################
# Column, MultiColumn, and Table Transform Functions
################################################################################
class ColumnTransformFn(Protocol):
    """Callback protocol defining a per-column transformation function."""

    def __call__(self, col: pd.Series, params: TransformParams) -> pd.Series:
        """Create a callable."""
        ...


class TableTransformFn(Protocol):
    """Callback protocol defining a per-table transformation function."""

    def __call__(self, df: pd.DataFrame, params: TransformParams) -> pd.DataFrame:
        """Create a callable."""
        ...


class MultiColumnTransformFn(Protocol):
    """Callback protocol defining a per-table transformation function."""

    def __call__(
        self, df: pd.DataFrame, params: MultiColumnTransformParams
    ) -> pd.DataFrame:
        """Create a callable."""
        ...


def multicol_transform_fn_factory(
    col_fn: ColumnTransformFn,
    drop=True,
) -> MultiColumnTransformFn:
    """A factory for creating a multi-column transform function."""

    class InnerMultiColumnTransformFn(
        Callable[[pd.DataFrame, MultiColumnTransformParams], pd.DataFrame]
    ):
        __name__ = col_fn.__name__ + "_multicol"

        def __call__(
            self, df: pd.DataFrame, params: MultiColumnTransformParams
        ) -> pd.DataFrame:
            drop_col: bool = drop
            for col_name in params:
                if col_name in df.columns:
                    logger.debug(f"Applying {col_fn.__name__} to {col_name}")
                    new_col = col_fn(col=df[col_name], params=params[col_name])
                    if drop_col:
                        df = df.drop(columns=col_name)
                    df = pd.concat([df, new_col], axis="columns")
                else:
                    logger.warning(
                        f"Expected column {col_name} not found in dataframe during "
                        f"application of {col_fn.__name__}."
                    )
            return df

    return InnerMultiColumnTransformFn()


def convert_units(col: pd.Series, params: UnitConversion) -> pd.Series:
    """Convert the units of and appropriately rename a column."""
    new_name = re.sub(pattern=params.pattern, repl=params.repl, string=col.name)
    # only apply the unit conversion if the column name matched the pattern
    if not re.match(pattern=params.pattern, string=col.name):
        logger.warning(
            f"{col.name} did not match the unit rename pattern. Check for typos "
            "and make sure you're applying the conversion to an appropriate column."
        )
    if col.name == new_name:
        logger.debug(f"Old and new column names are identical: {col.name}.")
    col = (params.multiplier * col) + params.adder
    col.name = new_name
    return col


convert_units_multicol = multicol_transform_fn_factory(convert_units)


def categorize_strings(col: pd.Series, params: StringCategories) -> pd.Series:
    """Impose a controlled vocabulary on freeform string column."""
    uncategorized_strings = set(col).difference(params.mapping)
    if uncategorized_strings:
        logger.warning(
            f"{col.name}: Found {len(uncategorized_strings)} uncategorized values: "
            f"{uncategorized_strings}"
        )
    col = col.map(params.mapping).astype(pd.StringDtype())
    col.loc[col == params.na_category] = pd.NA
    return col


categorize_strings_multicol = multicol_transform_fn_factory(categorize_strings)


def nullify_outliers(col: pd.Series, params: ValidRange) -> pd.Series:
    """Set any values outside the valid range to NA."""
    col = pd.to_numeric(col, errors="coerce")
    col[~col.between(params.lower_bound, params.upper_bound)] = np.nan
    return col


nullify_outliers_multicol = multicol_transform_fn_factory(nullify_outliers)


def normalize_strings(col: pd.Series, params: bool) -> pd.Series:
    """Derive a canonical version of the strings in the column.

    Transformations include:

    * Conversion to Pandas nullable String data type.
    * Removal of some non-printable characters.
    * Unicode composite character decomposition.
    * Translation to lower case.
    * Stripping of leading and trailing whitespace.
    * Compression of multiple consecutive whitespace characters to a single space.

    """
    return (
        col.astype(pd.StringDtype())
        .str.replace(r"[\x00-\x1f\x7f-\x9f]", "", regex=True)
        .str.normalize("NFKD")
        .str.lower()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


normalize_strings_multicol = multicol_transform_fn_factory(normalize_strings)


def correct_units(df: pd.DataFrame, params: UnitCorrections) -> pd.DataFrame:
    """Correct outlying values based on inferred discrepancies in reported units.

    In many cases we know that a particular column in the database should have a value
    within a particular range (e.g. the heat content of a ton of coal is a well defined
    physical quantity -- it can be 15 mmBTU/ton or 22 mmBTU/ton, but it can't be 1
    mmBTU/ton or 100 mmBTU/ton).

    Sometimes these fields are reported in the wrong units (e.g. kWh of electricity
    generated rather than MWh) resulting in several recognizable populations of reported
    values showing up at different ranges of value within the data. In cases where the
    unit conversion and range of valid values are such that these populations do not
    overlap, it's possible to convert them to the canonical units fairly unambiguously.

    This issue is especially common in the context of fuel attributes, because fuels are
    reported in terms of many different units. Because fuels with different units are
    often reported in the same column, and different fuels have different valid ranges
    of values, it's also necessary to be able to select only a subset of the data that
    pertains to a particular fuel. This means filtering based on another column, so the
    function needs to have access to the whole dataframe.

    for the values, and a list of factors by which we expect to see some of the data
    multiplied due to unit errors.  Data found in these "ghost" distributions are
    multiplied by the appropriate factor to bring them into the expected range.

    Data values which are not found in one of the acceptable multiplicative ranges are
    set to NA.

    """
    logger.info(f"Correcting units of {params.col} where {params.query}.")
    # Select a subset of the input dataframe to work on. E.g. only the heat content
    # column for coal records:
    selected = df.loc[df.query(params.query).index, params.col]
    not_selected = df[params.col].drop(index=selected.index)

    # Now, we only want to alter the subset of these values which, when transformed by
    # the unit conversion, lie in the range of valid values.
    for uc in params.unit_conversions:
        converted = convert_units(col=selected, params=uc)
        converted = nullify_outliers(col=converted, params=params.valid_range)
        selected = selected.where(converted.isna(), converted)

    # Nullify outliers that remain after the corrections have been applied.
    na_before = sum(selected.isna())
    selected = nullify_outliers(col=selected, params=params.valid_range)
    na_after = sum(selected.isna())
    total_nullified = na_after - na_before
    logger.info(
        f"{total_nullified}/{len(selected)} ({total_nullified/len(selected):.2%}) "
        "of records could not be corrected and were set to NA."
    )
    # Combine our cleaned up values with the other values we didn't select.
    df = df.copy()
    df[params.col] = pd.concat([selected, not_selected])
    return df


################################################################################
# TableTransformer classes
################################################################################
class AbstractTableTransformer(ABC):
    """An abstract base table transformer class.

    This class is not specific to FERC 1 and should be moved to a widely available
    module when this transformer design is mature and we start using it for other data
    sources.

    Only methods that are generally useful across data sources or that must be defined
    by child casses should be defined here.

    """

    table_id: enum.Enum
    """Name of the PUDL database table that this table transformer produces.

    Must be defined in the database schema / metadata. This ID is used to instantiate
    the appropriate :class:`TableTransformParams` object.
    """

    @cached_property
    def params(self) -> TableTransformParams:
        """Obtain table transform parameters based on the table ID."""
        return TableTransformParams.from_id(table_id=self.table_id)

    ################################################################################
    # Abstract methods that must be defined by subclasses
    @abstractmethod
    def transform(self, **kwargs) -> dict[str, pd.DataFrame]:
        """Apply all specified transformations to the appropriate input dataframes."""
        ...

    ################################################################################
    # Default method implementations which can be used or overridden by subclasses
    def rename_columns(
        self,
        df: pd.DataFrame,
        params: RenameColumns,
    ) -> pd.DataFrame:
        """Rename the whole collection of dataframe columns using input params.

        Log if there's any mismatch between the columns in the dataframe, and the
        columns that have been defined in the mapping for renaming.

        """
        logger.info(f"{self.table_id.value}: Renaming {len(params.columns)} columns.")
        df_col_set = set(df.columns)
        param_col_set = set(params.columns)
        if df_col_set != param_col_set:
            unshared_values = df_col_set.symmetric_difference(param_col_set)
            logger.warning(
                f"{self.table_id.value}: Discrepancy between dataframe columns and "
                "rename dictionary keys. \n"
                f"Unshared values: {unshared_values}"
            )
        return df.rename(columns=params.columns)

    def normalize_strings_multicol(
        self,
        df: pd.DataFrame,
        params: dict[str, bool],
    ) -> pd.DataFrame:
        """Method wrapper for string normalization."""
        logger.info(f"{self.table_id.value}: Normalizing freeform string columns.")
        return normalize_strings_multicol(df, params)

    def categorize_strings_multicol(
        self,
        df: pd.DataFrame,
        params: dict[str, StringCategories],
    ) -> pd.DataFrame:
        """Method wrapper for string categorization."""
        logger.info(
            f"{self.table_id.value}: Categorizing string columns using a controlled "
            "vocabulary."
        )
        return categorize_strings_multicol(df, params)

    def nullify_outliers_multicol(
        self,
        df: pd.DataFrame,
        params: dict[str, ValidRange],
    ) -> pd.DataFrame:
        """Method wrapper for nullifying outlying values."""
        logger.info(f"{self.table_id.value}: Nullifying outlying values.")
        return nullify_outliers_multicol(df, params)

    def convert_units_multicol(
        self,
        df: pd.DataFrame,
        params: dict[str, UnitConversion],
    ) -> pd.DataFrame:
        """Method wrapper for columnwise unit conversions."""
        logger.info(
            f"{self.table_id.value}: Converting units and renaming columns accordingly."
        )
        return convert_units_multicol(df, params)


class Ferc1AbstractTableTransformer(AbstractTableTransformer):
    """An abstract class defining methods common to many FERC Form 1 tables.

    This subclass remains abstract because it does not define the required transform()
    abstractmethod.

    """

    table_id: Ferc1TableId

    @abstractmethod
    def transform(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Abstract FERC Form 1 specific transformation method.

        This method primarily exists to define the FERC 1 specific call signature.

        Params:
            raw_xbrl_instant: Table representing raw instantaneous XBRL facts.
            raw_xbrl_duration: Table representing raw duration XBRL facts.
            raw_dbf: Raw Visual FoxPro database table.

        Returns:
            A single transformed table concatenating multiple years of cleaned data
            derived from the raw DBF and/or XBRL inputs.

        """
        ...

    def concat_dbf_xbrl(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Process the raw data until the XBRL and DBF inputs have been unified."""
        processed_dbf = self.process_dbf(raw_dbf)
        processed_xbrl = self.process_xbrl(raw_xbrl_instant, raw_xbrl_duration)
        logger.info(f"{self.table_id.value}: Concatenating DBF + XBRL dataframes.")
        return pd.concat([processed_dbf, processed_xbrl]).reset_index(drop=True)

    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """DBF-specific transformations that take place before concatenation."""
        logger.info(f"{self.table_id.value}: Processing DBF data pre-concatenation.")
        return (
            self.drop_footnote_columns_dbf(raw_dbf)
            .pipe(self.rename_columns, params=self.params.rename_columns.dbf)
            .pipe(self.assign_record_id, source_ferc1=Ferc1Source.DBF)
            .pipe(self.drop_unused_original_columns_dbf)
        )

    def process_xbrl(
        self,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """XBRL-specific transformations that take place before concatenation."""
        logger.info(f"{self.table_id.value}: Processing XBRL data pre-concatenation.")
        return (
            self.merge_xbrl_instant_and_duration_tables(
                raw_xbrl_instant, raw_xbrl_duration
            )
            .pipe(self.rename_columns, params=self.params.rename_columns.xbrl)
            .pipe(self.assign_record_id, source_ferc1=Ferc1Source.XBRL)
            .pipe(self.assign_utility_id_ferc1_xbrl)
        )

    def merge_xbrl_instant_and_duration_tables(
        self,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge the XBRL instand and duration tables into a single dataframe.

        FERC1 XBRL instant period signifies that it is true as of the reported date,
        while a duration fact pertains to the specified time period. The ``date`` column
        for an instant fact corresponds to the ``end_date`` column of a duration fact.

        Args:
            raw_xbrl_instant: table representing XBRL instant facts.
            raw_xbrl_duration: table representing XBRL duration facts.

        Returns:
            A unified table combining the XBRL duration and instant facts, if both types
            of facts were present. If either input dataframe is empty, the other
            dataframe is returned unchanged, except that several unused columns are
            dropped. If both input dataframes are empty, an empty dataframe is returned.

        """
        drop_cols = ["filing_name", "index"]
        # Ignore errors in case not all drop_cols are present.
        instant = raw_xbrl_instant.drop(columns=drop_cols, errors="ignore")
        duration = raw_xbrl_duration.drop(columns=drop_cols, errors="ignore")

        if instant.empty:
            logger.debug(
                f"{self.table_id.value}: No XBRL instant table found, returning the "
                "duration table."
            )
            return duration
        if duration.empty:
            logger.debug(
                f"{self.table_id.value}: No XBRL duration table found, returning "
                "instant table."
            )
            return instant

        instant_axes = [col for col in raw_xbrl_instant.columns if col.endswith("Axis")]
        duration_axes = [
            col for col in raw_xbrl_duration.columns if col.endswith("Axis")
        ]
        if set(instant_axes) != set(duration_axes):
            raise ValueError(
                f"{self.table_id.value}: Instant and Duration XBRL Axes do not match.\n"
                f"    instant: {instant_axes}\n"
                f"    duration: {duration_axes}"
            )

        return pd.merge(
            instant,
            duration,
            how="outer",
            left_on=["date", "entity_id", "ReportYear"] + instant_axes,
            right_on=["end_date", "entity_id", "ReportYear"] + duration_axes,
            validate="1:1",
        )

    def drop_footnote_columns_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop DBF footnote reference columns, which all end with _f."""
        logger.debug(f"{self.table_id.value}: Dropping DBF footnote columns.")
        return df.drop(columns=df.filter(regex=r".*_f$").columns)

    def source_table_id(self, source_ferc1: Ferc1Source) -> str:
        """Look up the ID of the raw data source table."""
        return TABLE_NAME_MAP[self.table_id.value][source_ferc1.value]

    def source_table_primary_key(self, source_ferc1: Ferc1Source) -> list[str]:
        """Look up the pre-renaming source table primary key columns."""
        if source_ferc1 == Ferc1Source.DBF:
            pk_cols = [
                "report_year",
                "report_prd",
                "respondent_id",
                "spplmnt_num",
                "row_number",
            ]
        else:
            assert source_ferc1 == Ferc1Source.XBRL
            cols = self.params.rename_columns.xbrl.columns
            pk_cols = ["ReportYear", "entity_id"]
            # Sort to avoid dependence on the ordering of rename_columns.
            # Doing the sorting here because we have a particular ordering
            # hard coded for the DBF primary keys.
            pk_cols += sorted(col for col in cols if col.endswith("Axis"))
        return pk_cols

    def renamed_table_primary_key(self, source_ferc1: Ferc1Source) -> list[str]:
        """Look up the post-renaming primary key columns."""
        if source_ferc1 == Ferc1Source.DBF:
            cols = self.params.rename_columns.dbf.columns
        else:
            assert source_ferc1 == Ferc1Source.XBRL
            cols = self.params.rename_columns.xbrl.columns
        pk_cols = self.source_table_primary_key(source_ferc1=source_ferc1)
        # Translate to the renamed columns
        return [cols[col] for col in pk_cols]

    def drop_unused_original_columns_dbf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove residual DBF specific columns."""
        unused_cols = [
            "report_prd",
            "spplmnt_num",
            "row_number",
            "row_seq",
            "row_prvlg",
        ]
        logger.debug(
            f"{self.table_id.value}: Dropping unused DBF structural columns: "
            f"{unused_cols}"
        )
        missing_cols = set(unused_cols).difference(df.columns)
        if missing_cols:
            raise ValueError(
                f"{self.table_id.value}: Trying to drop missing original DBF columns:"
                f"{missing_cols}"
            )
        return df.drop(columns=unused_cols)

    def assign_record_id(
        self, df: pd.DataFrame, source_ferc1: Ferc1Source
    ) -> pd.DataFrame:
        """Add a column identifying the original source record for each row.

        It is often useful to be able to tell exactly which record in the FERC Form 1
        database a given record within the PUDL database came from.

        Within each FERC Form 1 DBF table, each record is supposed to be uniquely
        identified by the combination of: report_year, report_prd, utility_id_ferc1,
        spplmnt_num, row_number.

        The FERC Form 1 XBRL tables do not have these supplement and row number
        columns, so we construct an id based on:
        report_year, entity_id, and the primary key columns of the XBRL table

        Args:
            df: table to assign `record_id` to
            table_name: name of table
            source_ferc1: data source of raw ferc1 database.

        Raises:
            ValueError: If any of the primary key columns are missing from the DataFrame
                being processed.
            ValueError: If there are any null values in the primary key columns.
            ValueError: If the resulting `record_id` column is non-unique.
        """
        logger.debug(
            f"{self.table_id.value}: Assigning {source_ferc1.value} source record IDs."
        )
        pk_cols = self.renamed_table_primary_key(source_ferc1)
        missing_pk_cols = set(pk_cols).difference(df.columns)
        if missing_pk_cols:
            raise ValueError(
                f"{self.table_id.value} ({source_ferc1.value}): Missing primary key "
                "columns in dataframe while assigning source record_id: "
                f"{missing_pk_cols}"
            )
        if df[pk_cols].isnull().any(axis=None):
            raise ValueError(
                f"{self.table_id.value} ({source_ferc1.value}): Found null primary key "
                "values.\n"
                f"{df[pk_cols].isnull().any()}"
            )
        df = df.assign(
            source_table_id=self.source_table_id(source_ferc1),
            record_id=lambda x: x.source_table_id.str.cat(
                x[pk_cols].astype(str), sep="_"
            ),
        ).drop(columns=["source_table_id"])
        dupe_ids = df.record_id[df.record_id.duplicated()].values
        if dupe_ids.any():
            logger.warning(
                f"{self.table_id.value}: Found {len(dupe_ids)} duplicate record_ids. "
                f"{dupe_ids}."
            )
        return df

    def assign_utility_id_ferc1_xbrl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Assign utility_id_ferc1.

        This is a temporary solution until we have real ID mapping working for the XBRL
        entity IDs. See https://github.com/catalyst-cooperative/pudl/issue/1705

        Note that in some cases this will create collisions with the existing
        utility_id_ferc1 values.
        """
        logger.warning(f"{self.table_id.value}: USING DUMMY UTILITY_ID_FERC1 IN XBRL.")
        return df.assign(
            utility_id_ferc1=lambda x: x.entity_id.str.replace(r"^C", "", regex=True)
            .str.lstrip("0")
            .astype("Int64")
        )

    def correct_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all specified unit corrections to the table."""
        logger.info(
            f"{self.table_id.value}: Correcting inferred non-standard column units."
        )
        for uc in self.params.correct_units:
            df = correct_units(df, uc)
        return df

    def enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop columns not in the DB schema and enforce specified types."""
        resource = Package.from_resource_ids().get_resource(self.table_id.value)
        expected_cols = pd.Index(resource.get_field_names())
        missing_cols = list(expected_cols.difference(df.columns))
        if missing_cols:
            raise ValueError(
                f"{self.table_id.value}: Missing columns found when enforcing table "
                f"schema: {missing_cols}"
            )
        return resource.format_df(df)


class FuelFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """A table transformer specific to the ``fuel_ferc1`` table.

    The ``fuel_ferc1`` table reports data about fuel consumed by large thermal power
    plants that report in the ``plants_steam_ferc1`` table.  Each record in the steam
    table is typically associated with several records in the fuel table, with each fuel
    record reporting data for a particular type of fuel consumed by that plant over the
    course of a year. The fuel table presents several challenges.

    The type of fuel, which is part of the primary key for the table, is a freeform
    string with hundreds of different nonstandard values. These strings are categorized
    manually and converted to ``fuel_type_code_pudl``. Some values cannot be categorized
    and are set to ``other``. In other string categorizations we set the unidentifiable
    values to NA, but in this table the fuel type is part of the primary key and primary
    keys cannot contain NA values.

    This simplified categorization occasionally results in records with duplicate
    primary keys. In those cases the records are aggregated into a single record if they
    have the same apparent physical units. If the fuel units are different, only the
    first record is retained.

    Several columns have unspecified, inconsistent, fuel-type specific units of measure
    associated with them. In order for records to be comparable and aggregatable, we
    have to infer and standardize these units.

    In the raw FERC Form 1 data there is a ``fuel_units`` column which describes the
    units of fuel delivered or consumed. Most commonly this is short tons for solid
    fuels (coal), thousands of cubic feet (Mcf) for gaseous fuels, and barrels (bbl) for
    liquid fuels.  However, the ``fuel_units`` column is also a freeform string with
    hundreds of nonstandard values which we have to manually categorize, and many of the
    values do not map directly to the most commonly used units for fuel quantities. E.g.
    some solid fuel quantities are reported in pounds, or thousands of pounds, not tons;
    some liquid fuels are reported in gallons or thousands of gallons, not barrels; and
    some gaseous fuels are reported in cubic feet not thousands of cubic feet.

    Two additional columns report fuel price per unit of heat content and fuel heat
    content per physical unit of fuel. The units of those columns are not explicitly
    reported, vary by fuel, and are inconsistent within individual fuel types.

    We adopt standardized units and attempt to convert all reported values in the fuel
    table into those units. For physical fuel units we adopt those that are used by the
    EIA: short tons (tons) for solid fuels, barrels (bbl) for liquid fuels, and
    thousands of cubic feet (mcf) for gaseous fuels. For heat content per (physical)
    unit of fuel, we use millions of British thermal units (mmbtu). All fuel prices are
    converted to US dollars, while many are reported in cents.

    Because the reported fuel price and heat content units are implicit, we have to
    infer them based on observed values. This is only possible because these quantities
    are ratios with well defined ranges of valid values. The common units that we
    observe and attempt to standardize include:

    * coal: primarily BTU/pound, but also MMBTU/ton and MMBTU/pound.
    * oil: primarily BTU/gallon.
    * gas: reported in a mix of MMBTU/cubic foot, and MMBTU/thousand cubic feet.

    Steps to take, in order:

    * Convert units in per-unit columns and rename the columns
    * Normalize freeform strings (fuel type and fuel units)
    * Categorize strings in fuel type and fuel unit columns
    * Standardize physical fuel units based on reported units (tons, mcf, bbl)
    * Remove fuel_units column
    * Convert heterogenous fuel price and heat content columns to their aspirational
      units.
    * Apply fuel unit corrections to fuel price and heat content columns based on
      observed clustering of values.

    """

    table_id: Ferc1TableId = Ferc1TableId.FUEL_FERC1

    def transform(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Transform the fuel_ferc1 table.

        Params:
            raw_xbrl_instant: Table representing raw instantaneous XBRL facts.
            raw_xbrl_duration: Table representing raw duration XBRL facts.
            raw_dbf: Raw Visual FoxPro database table.

        Returns:
            A single transformed table concatenating multiple years of cleaned data
            derived from the raw DBF and/or XBRL inputs.

        """
        return (
            self.concat_dbf_xbrl(
                raw_dbf=raw_dbf,
                raw_xbrl_instant=raw_xbrl_instant,
                raw_xbrl_duration=raw_xbrl_duration,
            )
            .pipe(self.drop_null_data_rows)
            .pipe(self.correct_units)
            .pipe(self.enforce_schema)
        )

    def process_dbf(self, raw_dbf: pd.DataFrame) -> pd.DataFrame:
        """Start with inherited method and do some fuel-specific processing.

        Mostly this needs to do extra work because of the linkage between the fuel_ferc1
        and plants_steam_ferc1 tables, and because the fuel type column is both a big
        mess of freeform strings and part of the primary key.
        """
        df = (
            super()
            .process_dbf(raw_dbf)
            .pipe(self.convert_units_multicol, params=self.params.convert_units)
            .pipe(self.normalize_strings_multicol, params=self.params.normalize_strings)
            .pipe(
                self.categorize_strings_multicol, params=self.params.categorize_strings
            )
            .pipe(self.standardize_physical_fuel_units)
        )
        return df

    def process_xbrl(
        self, raw_xbrl_instant: pd.DataFrame, raw_xbrl_duration: pd.DataFrame
    ) -> pd.DataFrame:
        """Special pre-concat treatment of the fuel_ferc1 table.

        This is necessary because the fuel type is a messy freeform string column that
        needs to be cleaned up, and is also (insanely) a primary key column for the
        table, and required for merging the fuel_ferc1 and plants_steam_ferc1 tables.
        This means that we can't assign a record ID until the fuel types have been
        cleaned up. Additionally the string categorization results in a number of
        duplicate fuel records which need to be aggregated.
        """
        return (
            self.merge_xbrl_instant_and_duration_tables(
                raw_xbrl_instant, raw_xbrl_duration
            )
            .pipe(self.rename_columns, params=self.params.rename_columns.xbrl)
            .pipe(self.convert_units_multicol, params=self.params.convert_units)
            .pipe(self.normalize_strings_multicol, params=self.params.normalize_strings)
            .pipe(
                self.categorize_strings_multicol, params=self.params.categorize_strings
            )
            .pipe(self.standardize_physical_fuel_units)
            .pipe(self.aggregate_duplicate_fuel_types_xbrl)
            .pipe(self.assign_utility_id_ferc1_xbrl)
            .pipe(self.assign_record_id, source_ferc1=Ferc1Source.XBRL)
        )

    def standardize_physical_fuel_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert reported fuel quantities to standard units depending on fuel type.

        Use the categorized fuel type and reported fuel units to convert all fuel
        quantities to the following standard units, depending on whether the fuel is a
        solid, liquid, or gas. When a single fuel reports its quantity in fundamentally
        different units, convert based on typical values. E.g. 19.85 MMBTU per ton of
        coal, 1.037 Mcf per MMBTU of natural gas, 7.46 barrels per ton of oil.

          * solid fuels (coal and waste): short tons [ton]
          * liquid fuels (oil): barrels [bbl]
          * gaseous fuels (gas): thousands of cubic feet [mcf]

        Columns to which these physical units apply:

          * fuel_consumed_units (tons, bbl, mcf)
          * fuel_cost_per_unit_burned (usd/ton, usd/bbl, usd/mcf)
          * fuel_cost_per_unit_delivered (usd/ton, usd/bbl, usd/mcf)

        One remaining challenge in this standardization is that nuclear fuel is reported
        in both mass of Uranium and fuel heat content, and it's unclear if there's any
        reasonable typical conversion between these units, since available heat content
        depends on the degree of U235 enrichement, the type of reactor, and whether the
        fuel is just Uranium, or a mix of Uranium and Plutonium from decommissioned
        nuclear weapons. See:

        https://world-nuclear.org/information-library/facts-and-figures/heat-values-of-various-fuels.aspx

        """
        df = df.copy()

        FuelFix = namedtuple("FuelFix", "fuel from_unit to_unit mult")
        fuel_fixes = [
            FuelFix("coal", "mmbtu", "ton", (1.0 / 19.85)),
            FuelFix("coal", "klbs", "ton", (1.0 / 2.0)),
            FuelFix("coal", "lbs", "ton", (1.0 / 2000.0)),
            FuelFix("coal", "btu", "ton", (1.0 / 19.85e6)),
            FuelFix("oil", "gal", "bbl", (1.0 / 42.0)),
            FuelFix("oil", "kgal", "bbl", (1000.0 / 42.0)),
            FuelFix("oil", "ton", "bbl", 7.46),
            FuelFix("gas", "mmbtu", "mcf", (1.0 / 1.037)),
            FuelFix("nuclear", "mwdth", "mwhth", 24.0),
            FuelFix("nuclear", "mmmbtu", "mwhth", (1.0 / 3.412142)),
            FuelFix("nuclear", "btu", "mwhth", (1.0 / 3412142)),
            FuelFix("nuclear", "grams", "kg", (1.0 / 1000)),
        ]
        for fix in fuel_fixes:
            fuel_mask = df.fuel_type_code_pudl == fix.fuel
            unit_mask = df.fuel_units == fix.from_unit
            df.loc[(fuel_mask & unit_mask), "fuel_consumed_units"] *= fix.mult
            df.loc[(fuel_mask & unit_mask), "fuel_cost_per_unit_burned"] /= fix.mult
            df.loc[(fuel_mask & unit_mask), "fuel_cost_per_unit_delivered"] /= fix.mult
            df.loc[(fuel_mask & unit_mask), "fuel_units"] = fix.to_unit

        # Set all remaining non-standard units and affected columns to NA.
        FuelAllowedUnits = namedtuple("FuelAllowedUnits", "fuel allowed_units")
        fuel_allowed_units = [
            FuelAllowedUnits("coal", ("ton",)),
            FuelAllowedUnits("oil", ("bbl",)),
            FuelAllowedUnits("gas", ("mcf",)),
            FuelAllowedUnits("nuclear", ("kg", "mwhth")),
            FuelAllowedUnits("waste", ("ton",)),
            # for fuel type "other" set all units to NA
            FuelAllowedUnits("other", ()),
        ]
        physical_units_cols = [
            "fuel_consumed_units",
            "fuel_cost_per_unit_burned",
            "fuel_cost_per_unit_delivered",
        ]
        for fau in fuel_allowed_units:
            fuel_mask = df.fuel_type_code_pudl == fau.fuel
            unit_mask = ~df.fuel_units.isin(fau.allowed_units)
            df.loc[(fuel_mask & unit_mask), physical_units_cols] = np.nan
            df.loc[(fuel_mask & unit_mask), "fuel_units"] = pd.NA

        return df

    def aggregate_duplicate_fuel_types_xbrl(
        self, fuel_xbrl: pd.DataFrame
    ) -> pd.DataFrame:
        """Aggregate the fuel records having duplicate primary keys."""
        pk_cols = self.renamed_table_primary_key(source_ferc1=Ferc1Source.XBRL)
        fuel_xbrl.loc[:, "fuel_units_count"] = fuel_xbrl.groupby(pk_cols, dropna=False)[
            "fuel_units"
        ].transform("nunique")

        # split
        dupe_mask = fuel_xbrl.duplicated(subset=pk_cols, keep=False)
        multi_unit_mask = fuel_xbrl.fuel_units_count != 1

        fuel_pk_dupes = fuel_xbrl[dupe_mask & ~multi_unit_mask].copy()
        fuel_multi_unit = fuel_xbrl[dupe_mask & multi_unit_mask].copy()
        fuel_non_dupes = fuel_xbrl[~dupe_mask & ~multi_unit_mask]

        logger.info(
            f"{self.table_id.value}: Aggregating {len(fuel_pk_dupes)} rows with "
            f"duplicate primary keys out of {len(fuel_xbrl)} total rows."
        )
        logger.info(
            f"{self.table_id.value}: Dropping {len(fuel_multi_unit)} records with "
            "inconsistent fuel units preventing aggregation "
            f"out of {len(fuel_xbrl)} total rows."
        )
        agg_row_fraction = (len(fuel_pk_dupes) + len(fuel_multi_unit)) / len(fuel_xbrl)
        if agg_row_fraction > 0.15:
            logger.error(
                f"{self.table_id.value}: {agg_row_fraction:.0%} of all rows are being "
                "aggregated. Higher than the allowed value of 15%!"
            )
        data_cols = [
            "fuel_consumed_units",
            "fuel_mmbtu_per_unit",
            "fuel_cost_per_unit_delivered",
            "fuel_cost_per_unit_burned",
            "fuel_cost_per_mmbtu",
            "fuel_cost_per_mwh",
            "fuel_mmbtu_per_mwh",
        ]
        # apply
        fuel_pk_dupes = pudl.helpers.sum_and_weighted_average_agg(
            df_in=fuel_pk_dupes,
            by=pk_cols + ["start_date", "end_date", "fuel_units"],
            sum_cols=["fuel_consumed_units"],
            wtavg_dict={
                k: "fuel_consumed_units"
                for k in data_cols
                if k != "fuel_consumed_units"
            },
        )
        # We can't aggregate data when fuel units are inconsistent, but we don't want
        # to lose the records entirely, so we'll keep the first one.
        fuel_multi_unit.loc[:, data_cols] = np.nan
        fuel_multi_unit = fuel_multi_unit.drop_duplicates(subset=pk_cols, keep="first")
        # combine
        return pd.concat([fuel_non_dupes, fuel_pk_dupes, fuel_multi_unit]).drop(
            columns=["fuel_units_count"]
        )

    def drop_null_data_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows in which all data columns are null.

        In the case of the fuel_ferc1 table, we drop any row where all the data columns
        are null AND there's a non-null value in the ``fuel_mmbtu_per_mwh`` column, as
        it typically indicates a "total" row for a plant. We also require a null value
        for the fuel_units and an "other" value for the fuel type.

        Right now this is extremely stringent and almost all rows are retained.

        """
        data_cols = [
            "fuel_consumed_units",
            "fuel_mmbtu_per_unit",
            "fuel_cost_per_unit_delivered",
            "fuel_cost_per_unit_burned",
            "fuel_cost_per_mmbtu",
            "fuel_cost_per_mwh",
        ]
        probably_totals_index = df[
            df[data_cols].isna().all(axis="columns")
            & df.fuel_mmbtu_per_mwh.notna()
            & (df.fuel_type_code_pudl == "other")
            & df.fuel_units.isna()
        ].index
        logger.info(
            f"{self.table_id.value}: Dropping "
            f"{len(probably_totals_index)}/{len(df)}"
            "rows of excessively null data."
        )
        return df.drop(index=probably_totals_index)


########################################################################################
########################################################################################
# Christina's Transformer Classes
########################################################################################
########################################################################################
class PlantsSteamFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Transformer class for the plants_steam_ferc1 table."""

    table_id: Ferc1TableId = Ferc1TableId.PLANTS_STEAM_FERC1

    def transform(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
        transformed_fuel: pd.DataFrame,
    ):
        """Perform table transformations for the plants_steam_ferc1 table."""
        self.plants_steam_combo = (
            self.concat_dbf_xbrl(
                raw_dbf=raw_dbf,
                raw_xbrl_instant=raw_xbrl_instant,
                raw_xbrl_duration=raw_xbrl_duration,
            )
            .pipe(self.normalize_strings_multicol, params=self.params.normalize_strings)
            .pipe(self.nullify_outliers_multicol, params=self.params.nullify_outliers)
            .pipe(
                self.categorize_strings_multicol, params=self.params.categorize_strings
            )
            .pipe(self.convert_units_multicol, params=self.params.convert_units)
            .pipe(self.correct_units)
            .pipe(pudl.helpers.convert_cols_dtypes, data_source="eia")
        )
        self.plants_steam_combo = self.plants_steam_combo.pipe(
            plants_steam_assign_plant_ids, ferc1_fuel_df=transformed_fuel
        ).pipe(self.enforce_schema)
        plants_steam_validate_ids(self.plants_steam_combo)
        return self.plants_steam_combo


##################################################################################
# OLD FERC TRANSFORM HELPER FUNCTIONS ############################################
##################################################################################
def unpack_table(ferc1_df, table_name, data_cols, data_rows):
    """Normalize a row-and-column based FERC Form 1 table.

    Pulls the named database table from the FERC Form 1 DB and uses the corresponding
    ferc1_row_map to unpack the row_number coded data.

    Args:
        ferc1_df (pandas.DataFrame): Raw FERC Form 1 DataFrame from the DB.
        table_name (str): Original name of the FERC Form 1 DB table.
        data_cols (list): List of strings corresponding to the original FERC Form 1
            database table column labels -- these are the columns of data that we are
            extracting (it can be a subset of the columns which are present in the
            original database).
        data_rows (list): List of row_names to extract, as defined in the FERC 1 row
            maps. Set to slice(None) if you want all rows.

    Returns:
        pandas.DataFrame

    """
    # Read in the corresponding row map:
    row_map = (
        pd.read_csv(
            importlib.resources.open_text(
                "pudl.package_data.ferc1.row_maps", f"{table_name}.csv"
            ),
            index_col=0,
            comment="#",
        )
        .copy()
        .transpose()
        .rename_axis(index="year_index", columns=None)
    )
    row_map.index = row_map.index.astype(int)

    # For each year, rename row numbers to variable names based on row_map.
    rename_dict = {}
    out_df = pd.DataFrame()
    for year in row_map.index:
        rename_dict = {v: k for k, v in dict(row_map.loc[year, :]).items()}
        _ = rename_dict.pop(-1, None)
        df = ferc1_df.loc[ferc1_df.report_year == year].copy()
        df.loc[:, "row_name"] = df.loc[:, "row_number"].replace(rename_dict)
        # The concatenate according to row_name
        out_df = pd.concat([out_df, df], axis="index")

    # Is this list of index columns universal? Or should they be an argument?
    idx_cols = ["respondent_id", "report_year", "report_prd", "spplmnt_num", "row_name"]
    logger.info(
        f"{len(out_df[out_df.duplicated(idx_cols)])/len(out_df):.4%} "
        f"of unpacked records were duplicates, and discarded."
    )
    # Index the dataframe based on the list of index_cols
    # Unstack the dataframe based on variable names
    out_df = (
        out_df.loc[:, idx_cols + data_cols]
        # These lost records should be minimal. If not, something's wrong.
        .drop_duplicates(subset=idx_cols)
        .set_index(idx_cols)
        .unstack("row_name")
        .loc[:, (slice(None), data_rows)]
    )
    return out_df


def cols_to_cats(df, cat_name, col_cats):
    """Turn top-level MultiIndex columns into a categorial column.

    In some cases FERC Form 1 data comes with many different types of related values
    interleaved in the same table -- e.g. current year and previous year income -- this
    can result in DataFrames that are hundreds of columns wide, which is unwieldy. This
    function takes those top level MultiIndex labels and turns them into categories in a
    single column, which can be used to select a particular type of report.

    Args:
        df (pandas.DataFrame): the dataframe to be simplified.
        cat_name (str): the label of the column to be created indicating what
            MultiIndex label the values came from.
        col_cats (dict): a dictionary with top level MultiIndex labels as keys,
            and the category to which they should be mapped as values.

    Returns:
        pandas.DataFrame: A re-shaped/re-labeled dataframe with one fewer levels of
        MultiIndex in the columns, and an additional column containing the assigned
        labels.

    """
    out_df = pd.DataFrame()
    for col, cat in col_cats.items():
        logger.info(f"Col: {col}, Cat: {cat}")
        tmp_df = df.loc[:, col].copy().dropna(how="all")
        tmp_df.loc[:, cat_name] = cat
        out_df = pd.concat([out_df, tmp_df])
    return out_df.reset_index()


def _clean_cols(df, table_name):
    """Adds a FERC record ID and drop FERC columns not to be loaded into PUDL.

    It is often useful to be able to tell exactly which record in the FERC Form 1
    database a given record within the PUDL database came from. Within each FERC Form 1
    table, each record is supposed to be uniquely identified by the combination of:
    report_year, report_prd, respondent_id, spplmnt_num, row_number.

    So this function takes a dataframe, checks to make sure it contains each of those
    columns and that none of them are NULL, and adds a new column to the dataframe
    containing a string of the format:

    {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}

    In some PUDL FERC Form 1 tables (e.g. plant_in_service_ferc1) a single row is
    re-organized into several new records in order to normalize the data and ensure it
    is stored in a "tidy" format. In such cases each of the resulting PUDL records will
    have the same ``record_id``.  Otherwise, the ``record_id`` is expected to be unique
    within each FERC Form 1 table. However there are a handful of cases in which this
    uniqueness constraint is violated due to data reporting issues in FERC Form 1.

    In addition to those primary key columns, there are some columns which are not
    meaningful or useful in the context of PUDL, but which show up in virtually every
    FERC table, and this function drops them if they are present. These columns include:
    row_prvlg, row_seq, item, record_number (a temporary column used in plants_small)
    and all the footnote columns, which end in "_f".

    TODO: remove in xbrl transition. migrated this functionality into
    ``assign_record_id()``. The last chunk of this function that removes the "_f"
    columns should be abandoned in favor of using the metadata to ensure the
    tables have all/only the correct columns.

    Args:
        df (pandas.DataFrame): The DataFrame in which the function looks for columns
            for the unique identification of FERC records, and ensures that those
            columns are not NULL.
        table_name (str): The name of the table that we are cleaning.

    Returns:
        pandas.DataFrame: The same DataFrame with a column appended containing a string
        of the format
        {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}

    Raises:
        AssertionError: If the table input contains NULL columns

    """
    # Make sure that *all* of these columns exist in the proffered table:
    for field in [
        "report_year",
        "report_prd",
        "respondent_id",
        "spplmnt_num",
        "row_number",
    ]:
        if field in df.columns:
            if df[field].isnull().any():
                raise AssertionError(
                    f"Null field {field} found in ferc1 table {table_name}."
                )

    # Create a unique inter-year FERC table record ID:
    df["record_id"] = (
        table_name
        + "_"
        + df.report_year.astype(str)
        + "_"
        + df.report_prd.astype(str)
        + "_"
        + df.respondent_id.astype(str)
        + "_"
        + df.spplmnt_num.astype(str)
    )
    # Because of the way we are re-organizing columns and rows to create well
    # normalized tables, there may or may not be a row number available.
    if "row_number" in df.columns:
        df["record_id"] = df["record_id"] + "_" + df.row_number.astype(str)

        # Check to make sure that the generated record_id is unique... since
        # that's kind of the whole point. There are couple of genuine bad
        # records here that are taken care of in the transform step, so just
        # print a warning.
        n_dupes = df.record_id.duplicated().values.sum()
        if n_dupes:
            dupe_ids = df.record_id[df.record_id.duplicated()].values
            logger.warning(
                f"{n_dupes} duplicate record_id values found "
                f"in pre-transform table {table_name}: {dupe_ids}."
            )
    # May want to replace this with always constraining the cols to the metadata cols
    # at the end of the transform step (or in rename_columns if we don't need any
    # temp columns)
    # Drop any _f columns... since we're not using the FERC Footnotes...
    # Drop columns and don't complain about it if they don't exist:
    no_f = [c for c in df.columns if not re.match(".*_f$", c)]
    df = (
        df.loc[:, no_f]
        .drop(
            [
                "spplmnt_num",
                "row_number",
                "row_prvlg",
                "row_seq",
                "report_prd",
                "item",
                "record_number",
            ],
            errors="ignore",
            axis="columns",
        )
        .rename(columns={"respondent_id": "utility_id_ferc1"})
    )
    return df


########################################################################################
# Old per-table transform functions
########################################################################################
def plants_small(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 plant_small data for loading into PUDL Database.

    This FERC Form 1 table contains information about a large number of small plants,
    including many small hydroelectric and other renewable generation facilities.
    Unfortunately the data is not well standardized, and so the plants have been
    categorized manually, with the results of that categorization stored in an Excel
    spreadsheet. This function reads in the plant type data from the spreadsheet and
    merges it with the rest of the information from the FERC DB based on record number,
    FERC respondent ID, and report year. When possible the FERC license number for small
    hydro plants is also manually extracted from the data.

    This categorization will need to be renewed with each additional year of FERC data
    we pull in. As of v0.1 the small plants have been categorized for 2004-2015.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.
    """
    # grab table from dictionary of dfs
    ferc1_small_df = ferc1_dbf_raw_dfs["plants_small_ferc1"]
    # Standardize plant_name_raw capitalization and remove leading/trailing
    # white space -- necesary b/c plant_name_raw is part of many foreign keys.
    ferc1_small_df = pudl.helpers.simplify_strings(
        ferc1_small_df, ["plant_name", "kind_of_fuel"]
    )

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_small_df = pudl.helpers.oob_to_nan(
        ferc1_small_df,
        cols=["yr_constructed"],
        lb=1850,
        ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
    )

    # Convert from cents per mmbtu to dollars per mmbtu to be consistent
    # with the f1_fuel table data. Also, let's use a clearer name.
    ferc1_small_df["fuel_cost_per_mmbtu"] = ferc1_small_df["fuel_cost"] / 100.0
    ferc1_small_df.drop("fuel_cost", axis=1, inplace=True)

    # Create a single "record number" for the individual lines in the FERC
    # Form 1 that report different small plants, so that we can more easily
    # tell whether they are adjacent to each other in the reporting.
    ferc1_small_df["record_number"] = (
        46 * ferc1_small_df["spplmnt_num"] + ferc1_small_df["row_number"]
    )

    # Unforunately the plant types were not able to be parsed automatically
    # in this table. It's been done manually for 2004-2015, and the results
    # get merged in in the following section.
    small_types_file = importlib.resources.open_binary(
        "pudl.package_data.ferc1", "small_plants_2004-2016.xlsx"
    )
    small_types_df = pd.read_excel(small_types_file)

    # Only rows with plant_type set will give us novel information.
    small_types_df.dropna(
        subset=[
            "plant_type",
        ],
        inplace=True,
    )
    # We only need this small subset of the columns to extract the plant type.
    small_types_df = small_types_df[
        [
            "report_year",
            "respondent_id",
            "record_number",
            "plant_name_clean",
            "plant_type",
            "ferc_license",
        ]
    ]

    # Munge the two dataframes together, keeping everything from the
    # frame we pulled out of the FERC1 DB, and supplementing it with the
    # plant_name, plant_type, and ferc_license fields from our hand
    # made file.
    ferc1_small_df = pd.merge(
        ferc1_small_df,
        small_types_df,
        how="left",
        on=["report_year", "respondent_id", "record_number"],
    )

    # Remove extraneous columns and add a record ID
    ferc1_small_df = _clean_cols(ferc1_small_df, "f1_gnrt_plant")

    # Standardize plant_name capitalization and remove leading/trailing white
    # space, so that plant_name matches formatting of plant_name_raw
    ferc1_small_df = pudl.helpers.simplify_strings(ferc1_small_df, ["plant_name_clean"])

    # in order to create one complete column of plant names, we have to use the
    # cleaned plant names when available and the orignial plant names when the
    # cleaned version is not available, but the strings first need cleaning
    ferc1_small_df["plant_name_clean"] = ferc1_small_df["plant_name_clean"].fillna(
        value=""
    )
    ferc1_small_df["plant_name_clean"] = ferc1_small_df.apply(
        lambda row: row["plant_name"]
        if (row["plant_name_clean"] == "")
        else row["plant_name_clean"],
        axis=1,
    )

    # now we don't need the uncleaned version anymore
    # ferc1_small_df.drop(['plant_name'], axis=1, inplace=True)

    ferc1_small_df.rename(
        columns={
            # FERC 1 DB Name      PUDL DB Name
            "plant_name": "plant_name_ferc1",
            "ferc_license": "ferc_license_id",
            "yr_constructed": "construction_year",
            "capacity_rating": "capacity_mw",
            "net_demand": "peak_demand_mw",
            "net_generation": "net_generation_mwh",
            "plant_cost": "total_cost_of_plant",
            "plant_cost_mw": "capex_per_mw",
            "operation": "opex_operations",
            "expns_fuel": "opex_fuel",
            "expns_maint": "opex_maintenance",
            "kind_of_fuel": "fuel_type",
            "fuel_cost": "fuel_cost_per_mmbtu",
        },
        inplace=True,
    )

    ferc1_transformed_dfs["plants_small_ferc1"] = ferc1_small_df
    return ferc1_transformed_dfs


def plants_hydro(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 plant_hydro data for loading into PUDL Database.

    Standardizes plant names (stripping whitespace and Using Title Case). Also converts
    into our preferred units of MW and MWh.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.
    """
    # grab table from dictionary of dfs
    ferc1_hydro_df = (
        _clean_cols(ferc1_dbf_raw_dfs["plants_hydro_ferc1"], "f1_hydro")
        # Standardize plant_name capitalization and remove leading/trailing
        # white space -- necesary b/c plant_name is part of many foreign keys.
        .pipe(pudl.helpers.simplify_strings, ["plant_name"])
        .pipe(
            pudl.helpers.cleanstrings,
            ["plant_const"],
            [CONSTRUCTION_TYPE_CATEGORIES["categories"]],
            unmapped=pd.NA,
        )
        .assign(
            # Converting kWh to MWh
            net_generation_mwh=lambda x: x.net_generation / 1000.0,
            # Converting cost per kW installed to cost per MW installed:
            cost_per_mw=lambda x: x.cost_per_kw * 1000.0,
            # Converting kWh to MWh
            expns_per_mwh=lambda x: x.expns_kwh * 1000.0,
        )
        .pipe(
            pudl.helpers.oob_to_nan,
            cols=["yr_const", "yr_installed"],
            lb=1850,
            ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
        )
        .drop(columns=["net_generation", "cost_per_kw", "expns_kwh"])
        .rename(
            columns={
                # FERC1 DB          PUDL DB
                "plant_name": "plant_name_ferc1",
                "project_no": "project_num",
                "yr_const": "construction_year",
                "plant_kind": "plant_type",
                "plant_const": "construction_type",
                "yr_installed": "installation_year",
                "tot_capacity": "capacity_mw",
                "peak_demand": "peak_demand_mw",
                "plant_hours": "plant_hours_connected_while_generating",
                "favorable_cond": "net_capacity_favorable_conditions_mw",
                "adverse_cond": "net_capacity_adverse_conditions_mw",
                "avg_num_of_emp": "avg_num_employees",
                "cost_of_land": "capex_land",
                "cost_structure": "capex_structures",
                "cost_facilities": "capex_facilities",
                "cost_equipment": "capex_equipment",
                "cost_roads": "capex_roads",
                "cost_plant_total": "capex_total",
                "cost_per_mw": "capex_per_mw",
                "expns_operations": "opex_operations",
                "expns_water_pwr": "opex_water_for_power",
                "expns_hydraulic": "opex_hydraulic",
                "expns_electric": "opex_electric",
                "expns_generation": "opex_generation_misc",
                "expns_rents": "opex_rents",
                "expns_engineering": "opex_engineering",
                "expns_structures": "opex_structures",
                "expns_dams": "opex_dams",
                "expns_plant": "opex_plant",
                "expns_misc_plant": "opex_misc_plant",
                "expns_per_mwh": "opex_per_mwh",
                "expns_engnr": "opex_engineering",
                "expns_total": "opex_total",
                "asset_retire_cost": "asset_retirement_cost",
                "": "",
            }
        )
        .drop_duplicates(
            subset=[
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",
            ],
            keep=False,
        )
    )
    if ferc1_hydro_df["construction_type"].isnull().any():
        raise AssertionError(
            "NA values found in construction_type column during FERC1 hydro clean, add "
            "string to CONSTRUCTION_TYPES"
        )
    ferc1_hydro_df = ferc1_hydro_df.replace({"construction_type": "unknown"}, pd.NA)
    ferc1_transformed_dfs["plants_hydro_ferc1"] = ferc1_hydro_df
    return ferc1_transformed_dfs


def plants_pumped_storage(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 pumped storage data for loading into PUDL.

    Standardizes plant names (stripping whitespace and Using Title Case). Also converts
    into our preferred units of MW and MWh.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.
    """
    # grab table from dictionary of dfs
    ferc1_pump_df = (
        _clean_cols(
            ferc1_dbf_raw_dfs["plants_pumped_storage_ferc1"], "f1_pumped_storage"
        )
        # Standardize plant_name capitalization and remove leading/trailing
        # white space -- necesary b/c plant_name is part of many foreign keys.
        .pipe(pudl.helpers.simplify_strings, ["plant_name"])
        # Clean up the messy plant construction type column:
        .pipe(
            pudl.helpers.cleanstrings,
            ["plant_kind"],
            [CONSTRUCTION_TYPE_CATEGORIES["categories"]],
            unmapped=pd.NA,
        )
        .assign(
            # Converting from kW/kWh to MW/MWh
            net_generation_mwh=lambda x: x.net_generation / 1000.0,
            energy_used_for_pumping_mwh=lambda x: x.energy_used / 1000.0,
            net_load_mwh=lambda x: x.net_load / 1000.0,
            cost_per_mw=lambda x: x.cost_per_kw * 1000.0,
            expns_per_mwh=lambda x: x.expns_kwh * 1000.0,
        )
        .pipe(
            pudl.helpers.oob_to_nan,
            cols=["yr_const", "yr_installed"],
            lb=1850,
            ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
        )
        .drop(
            columns=[
                "net_generation",
                "energy_used",
                "net_load",
                "cost_per_kw",
                "expns_kwh",
            ]
        )
        .rename(
            columns={
                # FERC1 DB          PUDL DB
                "plant_name": "plant_name_ferc1",
                "project_number": "project_num",
                "tot_capacity": "capacity_mw",
                "project_no": "project_num",
                "plant_kind": "construction_type",
                "peak_demand": "peak_demand_mw",
                "yr_const": "construction_year",
                "yr_installed": "installation_year",
                "plant_hours": "plant_hours_connected_while_generating",
                "plant_capability": "plant_capability_mw",
                "avg_num_of_emp": "avg_num_employees",
                "cost_wheels": "capex_wheels_turbines_generators",
                "cost_land": "capex_land",
                "cost_structures": "capex_structures",
                "cost_facilties": "capex_facilities",
                "cost_wheels_turbines_generators": "capex_wheels_turbines_generators",
                "cost_electric": "capex_equipment_electric",
                "cost_misc_eqpmnt": "capex_equipment_misc",
                "cost_roads": "capex_roads",
                "asset_retire_cost": "asset_retirement_cost",
                "cost_of_plant": "capex_total",
                "cost_per_mw": "capex_per_mw",
                "expns_operations": "opex_operations",
                "expns_water_pwr": "opex_water_for_power",
                "expns_pump_strg": "opex_pumped_storage",
                "expns_electric": "opex_electric",
                "expns_misc_power": "opex_generation_misc",
                "expns_rents": "opex_rents",
                "expns_engneering": "opex_engineering",
                "expns_structures": "opex_structures",
                "expns_dams": "opex_dams",
                "expns_plant": "opex_plant",
                "expns_misc_plnt": "opex_misc_plant",
                "expns_producton": "opex_production_before_pumping",
                "pumping_expenses": "opex_pumping",
                "tot_prdctn_exns": "opex_total",
                "expns_per_mwh": "opex_per_mwh",
            }
        )
        .drop_duplicates(
            subset=[
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",
            ],
            keep=False,
        )
    )
    if ferc1_pump_df["construction_type"].isnull().any():
        raise AssertionError(
            "NA values found in construction_type column during FERC 1 pumped storage "
            "clean, add string to CONSTRUCTION_TYPES."
        )
    ferc1_pump_df = ferc1_pump_df.replace({"construction_type": "unknown"}, pd.NA)
    ferc1_transformed_dfs["plants_pumped_storage_ferc1"] = ferc1_pump_df
    return ferc1_transformed_dfs


def plant_in_service(ferc1_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 Plant in Service data for loading into PUDL.

    Re-organizes the original FERC Form 1 Plant in Service data by unpacking the rows as
    needed on a year by year basis, to organize them into columns. The "columns" in the
    original FERC Form 1 denote starting balancing, ending balance, additions,
    retirements, adjustments, and transfers -- these categories are turned into labels
    in a column called "amount_type". Because each row in the transformed table is
    composed of many individual records (rows) from the original table, row_number can't
    be part of the record_id, which means they are no longer unique. To infer exactly
    what record a given piece of data came from, the record_id and the row_map (found in
    the PUDL package_data directory) can be used.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of the transformed DataFrames.

    """
    pis_df = (
        unpack_table(
            ferc1_df=ferc1_raw_dfs["plant_in_service_ferc1"],
            table_name="f1_plant_in_srvce",
            data_rows=slice(None),  # Gotta catch 'em all!
            data_cols=[
                "begin_yr_bal",
                "addition",
                "retirements",
                "adjustments",
                "transfers",
                "yr_end_bal",
            ],
        )
        .pipe(  # Convert top level of column index into a categorical column:
            cols_to_cats,
            cat_name="amount_type",
            col_cats={
                "begin_yr_bal": "starting_balance",
                "addition": "additions",
                "retirements": "retirements",
                "adjustments": "adjustments",
                "transfers": "transfers",
                "yr_end_bal": "ending_balance",
            },
        )
        .rename_axis(columns=None)
        .pipe(_clean_cols, "f1_plant_in_srvce")
        .set_index(["utility_id_ferc1", "report_year", "amount_type", "record_id"])
        .reset_index()
    )

    # Get rid of the columns corresponding to "header" rows in the FERC
    # form, which should *never* contain data... but in about 2 dozen cases,
    # they do. See this issue on Github for more information:
    # https://github.com/catalyst-cooperative/pudl/issues/471
    pis_df = pis_df.drop(columns=pis_df.filter(regex=".*_head$").columns)

    ferc1_transformed_dfs["plant_in_service_ferc1"] = pis_df
    return ferc1_transformed_dfs


def purchased_power(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 pumped storage data for loading into PUDL.

    This table has data about inter-utility power purchases into the PUDL DB. This
    includes how much electricty was purchased, how much it cost, and who it was
    purchased from. Unfortunately the field describing which other utility the power was
    being bought from is poorly standardized, making it difficult to correlate with
    other data. It will need to be categorized by hand or with some fuzzy matching
    eventually.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of the transformed DataFrames.
    """
    # grab table from dictionary of dfs
    df = (
        _clean_cols(ferc1_dbf_raw_dfs["purchased_power_ferc1"], "f1_purchased_pwr")
        .rename(
            columns={
                "athrty_co_name": "seller_name",
                "sttstcl_clssfctn": "purchase_type_code",
                "rtsched_trffnbr": "tariff",
                "avgmth_bill_dmnd": "billing_demand_mw",
                "avgmth_ncp_dmnd": "non_coincident_peak_demand_mw",
                "avgmth_cp_dmnd": "coincident_peak_demand_mw",
                "mwh_purchased": "purchased_mwh",
                "mwh_recv": "received_mwh",
                "mwh_delvd": "delivered_mwh",
                "dmnd_charges": "demand_charges",
                "erg_charges": "energy_charges",
                "othr_charges": "other_charges",
                "settlement_tot": "total_settlement",
            }
        )
        .assign(  # Require these columns to numeric, or NaN
            billing_demand_mw=lambda x: pd.to_numeric(
                x.billing_demand_mw, errors="coerce"
            ),
            non_coincident_peak_demand_mw=lambda x: pd.to_numeric(
                x.non_coincident_peak_demand_mw, errors="coerce"
            ),
            coincident_peak_demand_mw=lambda x: pd.to_numeric(
                x.coincident_peak_demand_mw, errors="coerce"
            ),
        )
        .fillna(
            {  # Replace blanks w/ 0.0 in data columns.
                "purchased_mwh": 0.0,
                "received_mwh": 0.0,
                "delivered_mwh": 0.0,
                "demand_charges": 0.0,
                "energy_charges": 0.0,
                "other_charges": 0.0,
                "total_settlement": 0.0,
            }
        )
    )

    # Reencode the power purchase types:
    df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("purchased_power_ferc1")
        .encode(df)
    )

    # Drop records containing no useful data and also any completely duplicate
    # records -- there are 6 in 1998 for utility 238 for some reason...
    df = df.drop_duplicates().drop(
        df.loc[
            (
                (df.purchased_mwh == 0)
                & (df.received_mwh == 0)
                & (df.delivered_mwh == 0)
                & (df.demand_charges == 0)
                & (df.energy_charges == 0)
                & (df.other_charges == 0)
                & (df.total_settlement == 0)
            ),
            :,
        ].index
    )

    ferc1_transformed_dfs["purchased_power_ferc1"] = df

    return ferc1_transformed_dfs


def accumulated_depreciation(
    ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs
):
    """Transforms FERC Form 1 depreciation data for loading into PUDL.

    This information is organized by FERC account, with each line of the FERC Form 1
    having a different descriptive identifier like 'balance_end_of_year' or
    'transmission'.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of the transformed DataFrames.
    """
    # grab table from dictionary of dfs
    ferc1_apd_df = ferc1_dbf_raw_dfs["accumulated_depreciation_ferc1"]

    ferc1_acct_apd = FERC_DEPRECIATION_LINES.drop(["ferc_account_description"], axis=1)
    ferc1_acct_apd.dropna(inplace=True)
    ferc1_acct_apd["row_number"] = ferc1_acct_apd["row_number"].astype(int)

    ferc1_accumdepr_prvsn_df = pd.merge(
        ferc1_apd_df, ferc1_acct_apd, how="left", on="row_number"
    )
    ferc1_accumdepr_prvsn_df = _clean_cols(
        ferc1_accumdepr_prvsn_df, "f1_accumdepr_prvsn"
    )

    ferc1_accumdepr_prvsn_df.rename(
        columns={
            # FERC1 DB   PUDL DB
            "total_cde": "total"
        },
        inplace=True,
    )

    ferc1_transformed_dfs["accumulated_depreciation_ferc1"] = ferc1_accumdepr_prvsn_df

    return ferc1_transformed_dfs


def transform(
    ferc1_dbf_raw_dfs,
    ferc1_xbrl_raw_dfs,
    ferc1_settings: Ferc1Settings = Ferc1Settings(),
):
    """Transforms FERC 1.

    Args:
        ferc1_dbf_raw_dfs (dict): Dictionary pudl table names (keys) and raw DBF
            dataframes (values).
        ferc1_xbrl_raw_dfs (dict): Dictionary pudl table names with `_instant`
            or `_duration` (keys) and raw XRBL dataframes (values).
        ferc1_settings: Validated ETL parameters required by
            this data source.

    Returns:
        dict: A dictionary of the transformed DataFrames.

    """
    ferc1_tfr_classes = {
        # fuel must come before steam b/c fuel proportions are used to aid in
        # plant # ID assignment.
        # "fuel_ferc1": FuelFerc1,
        # "plants_small_ferc1": plants_small,
        # "plants_hydro_ferc1": plants_hydro,
        # "plants_pumped_storage_ferc1": plants_pumped_storage,
        # "plant_in_service_ferc1": plant_in_service,
        # "purchased_power_ferc1": purchased_power,
        # "accumulated_depreciation_ferc1": accumulated_depreciation,
    }
    # create an empty ditctionary to fill up through the transform fuctions
    ferc1_transformed_dfs = {}

    # for each ferc table,
    for table in ferc1_tfr_classes:
        if table in ferc1_settings.tables:
            logger.info(
                f"Transforming raw FERC Form 1 dataframe for loading into {table}"
            )

            ferc1_transformed_dfs[table] = ferc1_tfr_classes[table](
                table_name=table
            ).execute(
                raw_dbf=ferc1_dbf_raw_dfs.get(table),
                raw_xbrl_instant=ferc1_xbrl_raw_dfs.get(table).get("instant", None),
                raw_xbrl_duration=ferc1_xbrl_raw_dfs.get(table).get("duration", None),
            )

    if "plants_steam_ferc1" in ferc1_settings.tables:
        ferc1_transformed_dfs[
            "plants_steam_ferc1"
        ] = PlantsSteamFerc1TableTransformer().execute(
            raw_dbf=ferc1_dbf_raw_dfs.get("plants_steam_ferc1"),
            raw_xbrl_instant=ferc1_xbrl_raw_dfs.get("plants_steam_ferc1").get(
                "instant", None
            ),
            raw_xbrl_duration=ferc1_xbrl_raw_dfs.get("plants_steam_ferc1").get(
                "duration", None
            ),
            transformed_fuel=ferc1_transformed_dfs["fuel_ferc1"],
        )

    # convert types and return:
    return {
        name: convert_cols_dtypes(df, data_source="ferc1")
        for name, df in ferc1_transformed_dfs.items()
    }
