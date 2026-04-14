"""
Consolidate IRS county-to-county migration outflow files for 2010_2011 through
2022_2023 and export a single CSV using Polars.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import polars as pl

INPUT_COLUMNS = [
    "ORIGIN_STFIPS",
    "ORIGIN_COFIPS",
    "DESTINATION_STFIPS",
    "DESTINATION_COFIPS",
    "DESTINATION_STATE",
    "DESTINATION_NAME",
    "RETURNS",
    "EXEMPTIONS",
    "AGGREGATE_AGI",
]

METRIC_COLUMNS = ["RETURNS", "EXEMPTIONS", "AGGREGATE_AGI"]

OUTPUT_COLUMNS = [
    "ORIGIN_FIPS",
    "ORIGIN_NAME",
    "ORIGIN_STATE",
    "DESTINATION_FIPS",
    "DESTINATION_NAME",
    "DESTINATION_STATE",
    "EXEMPTIONS",
    "RETURNS",
    "ORIGIN_YEAR",
    "DESTINATION_YEAR",
    "AGGREGATE_AGI",
]

OUTPUT_CSV = Path(r"D:\OneDrive\ICLUS_v3\irs_raw_import_2010_2023.csv")
START_ORIGIN_YEAR = 2010
END_ORIGIN_YEAR = 2022

DATA_ROOT_CANDIDATES = [
    Path(r"D:\OneDrive\Dissertation\data\IRS"),
    Path(r"e:\Dissertation\data\IRS"),
]


def expected_file_for_year_pair(data_root: Path, origin_year: int) -> Tuple[int, int, Path]:
    destination_year = origin_year + 1
    o_year = str(origin_year)[-2:]
    d_year = str(destination_year)[-2:]
    file_path = data_root / f"{origin_year}_{destination_year}" / f"countyoutflow{o_year}{d_year}.csv"
    return origin_year, destination_year, file_path


def resolve_data_root() -> Path:
    for candidate in DATA_ROOT_CANDIDATES:
        if candidate.exists():
            return candidate
    roots = "\n".join(f"  - {candidate}" for candidate in DATA_ROOT_CANDIDATES)
    raise FileNotFoundError(
        "Could not find IRS data root in any known location:\n"
        f"{roots}"
    )


def validate_inputs(data_root: Path) -> List[Tuple[int, int, Path]]:
    year_files: List[Tuple[int, int, Path]] = []
    missing_pairs: List[Tuple[int, int, Path]] = []

    for origin_year in range(START_ORIGIN_YEAR, END_ORIGIN_YEAR + 1):
        pair = expected_file_for_year_pair(data_root=data_root, origin_year=origin_year)
        if pair[2].exists():
            year_files.append(pair)
        else:
            missing_pairs.append(pair)

    if missing_pairs:
        details = "\n".join(
            f"  - {origin}_{dest}: {file_path}"
            for origin, dest, file_path in missing_pairs
        )
        raise FileNotFoundError(
            "Missing expected IRS county outflow files for one or more year-pairs:\n"
            f"{details}"
        )

    return year_files


def load_and_transform(raw_file: Path, origin_year: int, destination_year: int) -> pl.DataFrame:
    schema_overrides = {col: pl.Utf8 for col in INPUT_COLUMNS}

    df = pl.read_csv(
        raw_file,
        has_header=False,
        skip_rows=1,
        new_columns=INPUT_COLUMNS,
        schema_overrides=schema_overrides,
        encoding="latin1",
        truncate_ragged_lines=True,
    )

    suppression_filters = [
        pl.col(col)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.to_lowercase()
        .ne("d")
        for col in METRIC_COLUMNS
    ]

    df = df.filter(pl.all_horizontal(suppression_filters))

    df = df.with_columns(
        [
            pl.col(col)
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.replace_all("r", "")
            .alias(col)
            for col in METRIC_COLUMNS
        ]
    )

    df = df.with_columns(
        [
            pl.col("ORIGIN_STFIPS").cast(pl.Utf8).str.zfill(2).alias("ORIGIN_STFIPS"),
            pl.col("ORIGIN_COFIPS").cast(pl.Utf8).str.zfill(3).alias("ORIGIN_COFIPS"),
            pl.col("DESTINATION_STFIPS").cast(pl.Utf8).str.zfill(2).alias("DESTINATION_STFIPS"),
            pl.col("DESTINATION_COFIPS").cast(pl.Utf8).str.zfill(3).alias("DESTINATION_COFIPS"),
            pl.col("DESTINATION_STATE").cast(pl.Utf8).str.to_uppercase().alias("DESTINATION_STATE"),
            pl.col("RETURNS").cast(pl.Int64, strict=False).alias("RETURNS"),
            pl.col("EXEMPTIONS").cast(pl.Int64, strict=False).alias("EXEMPTIONS"),
            (pl.col("AGGREGATE_AGI").cast(pl.Int64, strict=False) * 1000).alias("AGGREGATE_AGI"),
        ]
    )

    df = df.with_columns(
        [
            (pl.col("ORIGIN_STFIPS") + pl.col("ORIGIN_COFIPS")).alias("ORIGIN_FIPS"),
            (pl.col("DESTINATION_STFIPS") + pl.col("DESTINATION_COFIPS")).alias("DESTINATION_FIPS"),
            pl.lit(origin_year).alias("ORIGIN_YEAR"),
            pl.lit(destination_year).alias("DESTINATION_YEAR"),
            pl.lit("").alias("ORIGIN_NAME"),
            pl.lit("").alias("ORIGIN_STATE"),
        ]
    )

    return (
        df.drop(["ORIGIN_STFIPS", "ORIGIN_COFIPS", "DESTINATION_STFIPS", "DESTINATION_COFIPS"])
        .select(OUTPUT_COLUMNS)
    )


def main() -> None:
    data_root = resolve_data_root()
    print(f"Using IRS data root: {data_root}")

    year_files = validate_inputs(data_root=data_root)

    frames: List[pl.DataFrame] = []
    for origin_year, destination_year, raw_file in year_files:
        print(f"Processing {origin_year}_{destination_year}: {raw_file.name}")
        frame = load_and_transform(raw_file=raw_file, origin_year=origin_year, destination_year=destination_year)
        print(f"  Rows loaded: {frame.height}")
        frames.append(frame)

    if not frames:
        raise RuntimeError("No input files were processed.")

    final_df = pl.concat(frames, how="vertical")

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    final_df.write_csv(OUTPUT_CSV)

    print("Finished.")
    print(f"Output: {str(OUTPUT_CSV)}")
    print(f"Rows written: {int(final_df.height)}")
    origin_min = int(final_df["ORIGIN_YEAR"].min())
    origin_max = int(final_df["ORIGIN_YEAR"].max())
    destination_min = int(final_df["DESTINATION_YEAR"].min())
    destination_max = int(final_df["DESTINATION_YEAR"].max())
    print(
        "Year bounds: "
        f"ORIGIN_YEAR {origin_min}-{origin_max}, "
        f"DESTINATION_YEAR {destination_min}-{destination_max}"
    )


if __name__ == "__main__":
    main()
