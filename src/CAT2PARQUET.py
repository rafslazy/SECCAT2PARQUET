"""
CAT to Parquet Extractor
------------------------

Author: Miguel Ángel Freijedo
Description:
    Extracts fixed-width Spanish Cadastral (.CAT) tables and converts them
    into optimized Parquet datasets using PyArrow.

Usage:
    python cat_extract.py --input-folder /path/to/cat --table 11
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterator, Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


__author__ = "Miguel Ángel Freijedo"
__version__ = "1.1.0"
__license__ = "MIT"

SUPPORTED_TABLES = {"11", "13", "14", "15", "16", "17"}
CHUNK_SIZE = 1_000_000
ENCODING = "cp1252"


# --------------------------------------------------------------------------------------
# ARGUMENT PARSING
# --------------------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Spanish CAT cadastral tables to Parquet format."
    )

    parser.add_argument(
        "--input-folder",
        type=Path,
        required=True,
        help="Absolute path to folder containing .CAT files",
    )

    parser.add_argument(
        "--table",
        type=str,
        required=True,
        choices=SUPPORTED_TABLES,
        help="CAT table number to extract",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output parquet file (default: CAT{table}.parquet)",
    )

    return parser.parse_args()


# --------------------------------------------------------------------------------------
# TABLE CONFIGURATION
# --------------------------------------------------------------------------------------

def get_table_config(table_type: str) -> Dict:
    """
    Return positional metadata and transformation rules
    for a given CAT table type.
    """

    if table_type == "11":
        return {
            "positions": [
                2,23,25,28,30,44,50,52,77,80,83,123,153,158,163,188,192,193,
                197,198,203,207,215,240,245,247,250,252,255,260,265,295,
                305,312,319,326,333,342,352,581,601,666
            ],
            "columns": [
                "tipo_reg","unused0","cd","cmc","cn","pc","unused1","cp","np",
                "cmc2","cm","nm","nem","cv","tv","nv","pnp","plp","snp","slp",
                "km","bl","unused2","td","dp","dm","cma","czc","cpo","cpa",
                "cpaj","npa","sup","sct","ssr","sbr","sc","xcen","ycen",
                "unused3","rc_bice","n_bice","srs"
            ],
            "drop_cols": {"unused0","unused1","unused2","unused3","rc_bice","n_bice"},
            "numeric_cols": ["sup","sct","ssr","sbr","sc"],
            "centroid_cols": ["xcen","ycen"],
            "percent_cols": [],
        }
    elif table_type == "13":
        return {
            "positions": [
                2,23,25,28,30,44,48,50,52,77,80,83,123,153,158,163,188,192,
                193,197,198,203,215,240,295,299,300,307,312,409
            ],
            "columns": [
                "tipo_reg","UNUSED0","cd","cmc","cn","pc","cuc","UNUSED1","cp",
                "np","cmc2","cm","nm","nem","cv","tv","nv","pnp","plp","snp","slp",
                "km","UNUSED2","td","UNUSED3","ac","iacons","so","lf","UNUSED4","cucm"
            ],
            "drop_cols": {
                "UNUSED0", "UNUSED1", "UNUSED2", "UNUSED3", "UNUSED4", "cucm", "LineaCAT"
            },
            "numeric_cols": [],
            "centroid_cols": [],
            "percent_cols": [],
        }
    elif table_type == "14":
        return {
            "positions": [
                2,23,25,28,30,44,48,50,54,58,62,64,67,70,73,74,78,82,83,90,
                97,104,109,111
            ],
            "columns": [
                "tipo_reg","UNUSED0","cd","cmc","UNUSED1","pc","noec","UNUSED2",
                "nobf","cuc","bl","es","pt","pu","cd2","tr","ar","aec","ili",
                "stl","spt","sil","tip","UNUSED3","modl"
            ],
            "drop_cols": {
                "UNUSED0", "UNUSED1", "UNUSED2", "UNUSED3", "modl", "LineaCAT"
            },
            "numeric_cols": [],
            "centroid_cols": [],
            "percent_cols": [],
        }
    elif table_type == "15":
        return {
            "positions": [
                2,23,25,28,30,44,48,49,50,58,73,92,94,119,122,125,165,195,
                200,205,230,234,235,239,240,245,249,251,254,257,282,287,289,
                292,294,297,302,307,337,367,371,375,427,428,441,451,461
            ],
            "columns": [
                "tipo_reg","UNUSED0","cd","cmc","cn","pc","car","cc1","cc2",
                "nfbi","iia","nfv","cp","np","cmc2","cm","nm","nem","cv","tv",
                "nv","pnp","plp","snp","slp","km","bl","es","pt","pu","td","dp",
                "dm","cma","czc","cpo","cpa","cpaj","npa","UNUSED1","noe","ant",
                "UNUSED2","grbice/coduso","UNUSED3","sfc","sfs","cpt"
            ],
            "drop_cols": {
                "UNUSED0", "UNUSED1", "UNUSED2", "UNUSED3", "cpt", "LineaCAT"
            },
            "numeric_cols": [],
            "centroid_cols": [],
            "percent_cols": [],
        }
    elif table_type == "16":
        return {
            "positions": [
                2,23,25,28,30,44,48,50,54,58,64,113,117,123,172,176,182,231,235,
                241,290,294,300,349,353,359,408,412,418,467,471,477,526,530,536,
                585,589,595,644,648,654,703,707,713,762,766,772,821,825,831,880,
                884,890,939
            ],
            "columns": [
                "tipo_reg","UNUSED0","cd","cmc","UNUSED1","pc","noev","ccsp","nreg",
                "nc1","pr1","UNUSED3","nc2","pr2","UNUSED4","nc3","pr3","UNUSED5",
                "nc4","pr4","UNUSED6","nc5","pr5","UNUSED7","nc6","pr6","UNUSED8",
                "nc7","pr7","UNUSED9","nc8","pr8","UNUSED10","nc9","pr9","UNUSED11",
                "nc10","pr10","UNUSED12","nc11","pr11","UNUSED13","nc12","pr12","UNUSED14",
                "nc13","pr13","UNUSED15","nc14","pr14","UNUSED16","nc15","pr15","UNUSED17"
            ],
            "drop_cols": {
                "UNUSED0","UNUSED1","UNUSED2","UNUSED3","UNUSED4","UNUSED5","UNUSED6",
                "UNUSED7","UNUSED8","UNUSED9","UNUSED10","UNUSED11","UNUSED12",
                "UNUSED13","UNUSED14","UNUSED15","UNUSED16","UNUSED17","LineaCAT"
            },
            "numeric_cols": [],
            "centroid_cols": [],
            "percent_cols": [
                "pr1","pr2","pr3","pr4","pr5","pr6","pr7","pr8","pr9","pr10",
                "pr11","pr12","pr13","pr14","pr15"
            ],
        }
    elif table_type == "17":
        return {
            "positions": [
                2,23,25,28,30,44,48,50,54,55,65,67,107,109,126
            ],
            "columns": [
                "tipo_reg","UNUSED0","cd","cmc","cn","pc","cspr","UNUSED1","nobf",
                "tspr","ssp","ccc","dcc","ip","UNUSED2","modl"
            ],
            "drop_cols": {
                "UNUSED0", "UNUSED1", "UNUSED2", "modl", "LineaCAT"
            },
            "numeric_cols": [],
            "centroid_cols": [],
            "percent_cols": [],
        }

    raise NotImplementedError(f"Table {table_type} configuration not implemented.")


# --------------------------------------------------------------------------------------
# CORE LOGIC
# --------------------------------------------------------------------------------------

def split_fixed_width(
    line: str,
    positions: List[int],
    columns: List[str],
) -> Dict[str, str]:
    """
    Split a fixed-width CAT line into structured fields.
    """

    positions_full = [0] + positions + [len(line)]
    values = [
        line[positions_full[i]:positions_full[i + 1]].strip()
        for i in range(len(positions_full) - 1)
    ]

    return dict(zip(columns, values))


def read_cat_in_chunks(path: Path) -> Iterator[pd.DataFrame]:
    """
    Stream CAT file in chunks without loading it fully into memory.
    """

    with path.open("r", encoding=ENCODING, errors="replace") as f:
        buffer: List[str] = []

        for line in f:
            buffer.append(line.rstrip("\n"))
            if len(buffer) == CHUNK_SIZE:
                yield pd.DataFrame({"LineaCAT": buffer}, dtype="string")
                buffer.clear()

        if buffer:
            yield pd.DataFrame({"LineaCAT": buffer}, dtype="string")


def process_folder(
    input_folder: Path,
    table_type: str,
    output_file: Path,
) -> None:

    config = get_table_config(table_type)

    cat_files = list(input_folder.glob("*.cat"))
    total_files = len(cat_files)

    if total_files == 0:
        raise ValueError("No .CAT files found in input folder.")

    parquet_writer = None
    arrow_schema = None

    for index, cat_file in enumerate(cat_files, start=1):

        logging.info("Processing file %s (%d/%d)", cat_file.name, index, total_files)

        for chunk_number, chunk in enumerate(read_cat_in_chunks(cat_file), start=1):

            chunk = chunk[chunk["LineaCAT"].str.startswith(table_type)]

            if chunk.empty:
                continue

            records = [
                split_fixed_width(
                    line,
                    config["positions"],
                    config["columns"],
                )
                for line in chunk["LineaCAT"]
            ]

            df = pd.DataFrame.from_records(records)

            # Numeric casting
            for col in config["numeric_cols"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            for col in config["centroid_cols"]:
                df[col] = pd.to_numeric(df[col], errors="coerce") * 0.01

            for col in config["percent_cols"]:
                df[col] = pd.to_numeric(df[col], errors="coerce") * 0.001

            df.drop(columns=config["drop_cols"], inplace=True, errors="ignore")

            df = df.apply(
                lambda col: col.str.strip().replace("", pd.NA)
                if col.dtype == "string" else col
            )

            table = pa.Table.from_pandas(df, preserve_index=False)

            if arrow_schema is None:
                arrow_schema = table.schema
                parquet_writer = pq.ParquetWriter(
                    output_file,
                    arrow_schema,
                    compression="snappy",
                )

            table = table.cast(arrow_schema)
            parquet_writer.write_table(table)

            logging.info("  Chunk %d written", chunk_number)

    if parquet_writer:
        parquet_writer.close()

    logging.info("Parquet file generated at: %s", output_file)


# --------------------------------------------------------------------------------------
# ENTRY POINT
# --------------------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    args = parse_arguments()

    if not args.input_folder.exists():
        raise FileNotFoundError(f"Folder not found: {args.input_folder}")

    output_file = args.output or Path(f"CAT{args.table}.parquet")

    logging.info("Starting CAT → Parquet extraction")
    logging.info("Selected table: %s", args.table)

    process_folder(
        input_folder=args.input_folder,
        table_type=args.table,
        output_file=output_file,
    )

    logging.info("Process completed successfully.")


if __name__ == "__main__":
    main()
