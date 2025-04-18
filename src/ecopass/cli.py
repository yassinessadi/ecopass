#!/usr/bin/env python3
import os
import time
from datetime import datetime
from typing import List, Tuple, Dict

import click
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from rich.console import Console
from rich.table import Table

from ecopass.core.record_header import RecordHeader


# -------------------------------------------------
# Custom Display to show memory usage
# -------------------------------------------------
def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


# -------------------------------------------------------------------------------------------------------------
# Custom Display Function To show smooth animation of loading with ram usage and record counting in real-time
# -------------------------------------------------------------------------------------------------------------
def display_utils(batch_count: int, records: List[Tuple[int, bytes]], total_records: int) -> None:
    spinner_frames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']  # Spinner frames
    frame = spinner_frames[batch_count % len(spinner_frames)]
    message = (
        f"{frame} Processing batch {batch_count}: {len(records):,} records | "
        f"Total: {total_records:,} | "
        f"Memory: {get_memory_usage():.2f} MB | "
        f"Time: {datetime.now().strftime('%H:%M:%S')}"
    )
    click.echo(f"\r{message}", nl=False)


# -------------------------------------------------------------------------------
# Custom Time Format in second will output in terminal using click custom message
# -------------------------------------------------------------------------------
def format_time(seconds: float) -> str:
    """Format time duration in a human-readable format."""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"



def validate_header_size(ctx, param, value):
    if value not in {2, 4}:
        raise click.BadParameter('header-size must be 2 or 4.')
    return value
# ---------------------------------------------------------------------------------------------------------------------------------------
# User Input: Define File Paths Output_Dir with header-size and alignment and also custom batch-size to control the ram and cpu if needed
# ---------------------------------------------------------------------------------------------------------------------------------------
@click.command(context_settings=dict(help_option_names=['-h', '--help']), cls=click.Command, help='Process binary files with specified options.')
@click.option(
    '--input', 'path_input', required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Path to the binary file to process."
)
@click.option(
    '--output', 'path_output', required=True,
    type=click.Path(file_okay=False, writable=True),
    help="Output directory for encoded data as Parquet files."
)
@click.option(
    '--batch-size', default=500_000, show_default=True,
    type=click.IntRange(min=1),
    help="Batch size (number of records per batch)."
)
@click.option(
    '--header-size', default=2, show_default=True,
    type=click.INT,
    callback=validate_header_size,
    help=(
        "Length of the file header. The first 4 bits are always set to 3 (0011 in binary) "
        "indicating that this is a system record. The remaining bits contain the length of "
        "the file header record, unless the file is an indexed file type that does not include "
        "a separate index file (such as IDXFORMAT8); for those types of file, see Index File - "
        "File Header. If the maximum record length is less than 4095 bytes, the length is 126 "
        "and is held in the next 12 bits; otherwise it is 124 and is held in the next 28 bits. "
        "Hence, in a file where the maximum record length is less than 4095 bytes, this field "
        "contains x'30 7E 00 00'. Otherwise, this field contains x'30 00 00 7C'."
    )
)
@click.option(
    '--alignment', default=2, show_default=True,
    type=click.IntRange(min=2),
    help="Alignment to use with RecordHeader."
)
@click.option(
    '--debug', default=False, show_default=True,
    type=bool,
    help="Enable debug mode to display detailed logs and error messages for troubleshooting."
)
def main(path_input: str, path_output: str, batch_size: int, header_size: int, alignment: int, debug: bool) -> None:
    # -------------------------------------------
    # program start at start_time
    # -------------------------------------------
    start_time = time.time()

    # -------------------------------------------
    # Define Schema for pyarrow & pandas DataFrame
    # -------------------------------------------
    schema = pa.schema([
        ('rdw', pa.int32()),
        ('value', pa.binary())
    ])

    # ------------------------------------------------------------------------------------------
    # Read Records in Batches
    # ------------------------------------------------------------------------------------------
    # # batch size based on memory constraints -> 1_900_000 * 836 bytes = ~950 MB
    # ------------------------------------------------------------------------------------------
    # batch_size = 10_000
    # ------------------------------------------------------------------------------------------
    
    records = []
    total_records = 0
    batch_count = 0

    
    click.echo(f"\n{'='*50}")
    click.echo(f"Processing started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    click.echo(f"Batch size: {batch_size:,} records")
    click.echo(f"{'='*50}\n")

    # ------------------------------------------------------------------------------------------
    # with RecordHeader(filename=path_input, header_size=2, alignment=2) as record_header:
    # ------------------------------------------------------------------------------------------
    with RecordHeader(filename=path_input, header_size=header_size, alignment=alignment, debug=debug) as record_header:
        for length, data in record_header.read_records():
            records.append((length, data))
            total_records += 1
            
            # ---------------------------------------------
            # Write to Parquet when batch size is reached
            # ---------------------------------------------
            if len(records) >= batch_size:
                batch_count += 1

                # ------------------------------------------------
                # Console Ouput using Custom DIsplay using click
                # ------------------------------------------------
                display_utils(batch_count=batch_count, records=records, total_records=total_records)


                df = pd.DataFrame(records, columns=['rdw', 'value'])

                # ---------------------------------------------
                # Convert DataFrame to PyArrow Table
                # ---------------------------------------------
                table = pa.Table.from_pandas(df, schema=schema)
                # table = pa.Table.from_arrays([pa.array([length for length, _ in records]),pa.array([data for _, data in records])], schema=schema)
                # ---------------------------------------------
                # Write to Parquet file
                # ---------------------------------------------
                pq.write_to_dataset(table, root_path=path_output, partition_cols=['rdw'])

                # ---------------------------------------------
                # Clear records to free memory
                # ---------------------------------------------
                records.clear()

    # ---------------------------------------------
    # Write remaining records if any
    # ---------------------------------------------
    if records:
        batch_count += 1
        display_utils(batch_count=batch_count, records=records, total_records=total_records)
        df = pd.DataFrame(records, columns=['rdw', 'value'])
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_to_dataset(table, root_path=path_output, partition_cols=['rdw'])
        records.clear()
    # ---------------------------------------------
    # since the app convert into cli this will show the ouput of the program
    # Clear Jupyter Notebook Output (if applicable)
    # ---------------------------------------------
    # Final statistics
    # ---------------------------------------------
    end_time = time.time()
    duration = end_time - start_time

    # ---------------------------------------------
    # Initialize the console
    # ---------------------------------------------
    click.echo("\n")
    console = Console()

    # ---------------------------------------------
    # Create a table
    # ---------------------------------------------
    table = Table(title="Processing Summary")

    # ---------------------------------------------
    # Add columns
    # ---------------------------------------------
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")



    # ---------------------------------------------
    # Add rows
    # ---------------------------------------------
    table.add_row("[bold]Total records processed[/bold]", f"{total_records:,}")
    table.add_row("[bold]Number of [red]batches[/red][/bold]", f"{batch_count}")
    table.add_row("[bold]Average records per [red]batch[/red][/bold]", f"{total_records/batch_count:,.0f}")
    table.add_row("[bold]Total processing time[/bold]", format_time(duration))
    table.add_row("[bold]Processing rate[/bold]", f"{total_records/duration:,.0f} [red]R/S[/red]")
    table.add_row("[bold]Final memory usage[/bold]", f"{get_memory_usage():.2f} MB")
    table.add_row("[bold]Completed at[/bold]", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # ---------------------------------------------
    # Display the table
    # ---------------------------------------------
    console.print(table)

if __name__ == '__main__':
    main()