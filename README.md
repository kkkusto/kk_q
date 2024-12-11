"""
This module handles the archival of old Hive table partitions by moving them into a 'part2'
archival table and then removing them from the 'part1' source table. The process includes:
- Reading table names from a provided file
- Determining which partitions are older than a given threshold (e.g., 7 years)
- Creating a corresponding part2 table if not already present
- Copying and verifying data from the original partitions to the archival partitions
- If successful, removing the original partitions
- Logging operations and sending email notifications of the results
"""

import subprocess
import csv
import datetime
import uuid
import logging
import argparse
from utils import *
from email_helper import *

current_datetime = datetime.datetime.now()
current_datetime_str = current_datetime.strftime("%Y_%m_%d_%H%M%S")
log_filename = "pharmacy_tables_archival_{}.log".format(current_datetime_str)

# Configure logging
logging.basicConfig(
    filename=log_filename,  
    filemode='a',  
    format="%(asctime)s - %(levelname)s - %(message)s - Line:%(lineno)d",
    level=logging.DEBUG
)


def generate_unique_id():
    """
    Generate a unique identifier (UUID) for batch operations.

    Returns:
        str: A unique identifier (UUID string).
    """
    return str(uuid.uuid4())


def insert_to_part2_table(table_name_part1, partition, source_database, partitions_dict, beeline_str, 
                          insertion_timestamp, part1_column_names):
    """
    Insert data from a specific partition of a 'part1' table into the corresponding 'part2' archival table.

    Args:
        table_name_part1 (str): The name of the original table (without database prefix).
        partition (str): The partition string (e.g. "year=2012/month=01").
        source_database (str): The source database name.
        partitions_dict (dict): A dictionary of partition key-value pairs.
        beeline_str (str): The beeline connection string.
        insertion_timestamp (str): A timestamp string to use as a partition in the part2 table.
        part1_column_names (list[str]): The list of column names in the original table (excluding partition columns).

    Returns:
        tuple:
            A tuple (bool, str, str, int, int) where:
            - bool: Whether the insertion was successful.
            - str: Comments or status message.
            - str: The query used (if any).
            - int: Number of rows expected to copy from part1.
            - int: Number of rows actually copied into part2.
    """
    try:
        table_name_part2 = table_name_part1 + "_part2"
        joined_table_name_part1 = f"{source_database}.{table_name_part1}"
        joined_table_name_part2 = f"{source_database}.{table_name_part2}"
        condition = get_partition_condition(partitions_dict)

        # Remove partition columns from part1_column_names
        partitions_names = [key for key in partitions_dict.keys()]
        part1_column_names = [col for col in part1_column_names if col not in partitions_names]

        partitions_dict_part2 = partitions_dict.copy()
        partitions_dict_part2["part2_insertion_time_partition"] = insertion_timestamp

        # Get row count for original partition
        num_rows_partitions_part1 = get_row_count(joined_table_name_part1, beeline_str, condition)
        condition_part2 = get_partition_condition(partitions_dict_part2)

        if not num_rows_partitions_part1:
            count_failed_comment = (
                f"Failed to get counts for part1: {table_name_part1} "
                f"part2: {table_name_part2} table: {joined_table_name_part1} table remains"
            )
            print(count_failed_comment)
            logging.info(count_failed_comment)
            return False, count_failed_comment, "", 0, 0

        logging.info("Got row count for partition")
        print("Got row count for partition")

        # Insert data into part2 table
        beeline_cmd_insert_data = format_insert_data_query(
            joined_table_name_part1,
            joined_table_name_part2,
            partitions_dict_part2,
            condition,
            part1_column_names,
            beeline_str
        )

        logging.info(f"Inserting data into part2 table: {joined_table_name_part2}")
        print(f"Inserting data into part2 table: {joined_table_name_part2}")

        insert_sql_result = execute_beeline_sql(beeline_cmd_insert_data, beeline_str)
        num_rows_after_insert_part2 = int(get_row_count(joined_table_name_part2, beeline_str, condition_part2))
        num_rows_partitions_part1 = int(num_rows_partitions_part1)

        # Validate insertion
        if not insert_sql_result or (num_rows_after_insert_part2 != num_rows_partitions_part1):
            print("Error while copying data")
            logging.info("Rows got copied to part2 table: rollback required")
            print("Rows got copied to part2 table: rollback required")

            # Rollback
            logging.info(f"Deleting rows from part2 table: {joined_table_name_part2}")
            print(f"Deleting rows from part2 table: {joined_table_name_part2}")

            delete_rows_sql = format_delete_partition_query(joined_table_name_part2, partitions_dict_part2)
            delete_new_rows_result = execute_beeline_sql(delete_rows_sql, beeline_str)

            if not delete_new_rows_result:
                logging.info(f"Delete operation failed from part2 table: {joined_table_name_part2}")
                print(f"Deleting operation failed from part2 table: {joined_table_name_part2}")
                logging.info(f"Manual delete operation command: {delete_rows_sql}")
                print(f"Manual delete operation command: {delete_rows_sql}")
                return False, (
                    "Deleting operation failed from part2 table: {} - Manual delete operation command: {}".format(
                        joined_table_name_part2, delete_rows_sql
                    )
                ), delete_rows_sql, 0, 0

        logging.info(f"Successfully inserted data into part2 table: {joined_table_name_part2}")
        print(f"Successfully inserted data into part2 table: {joined_table_name_part2}")

        return True, (
            f"Successfully copied data to part2 table: {joined_table_name_part2} - "
            f"Use the following query to get data from part2 table: "
            f"{format_select_part2_query(joined_table_name_part2, condition_part2)}"
        ), "", num_rows_partitions_part1, num_rows_after_insert_part2

    except Exception as e:
        print(f"Error occurred for table {joined_table_name_part1}: {e}, partition: {partition}")
        logging.error(f"Error occurred for table {joined_table_name_part1}: {e}")
        return False, f"Error occurred for table {joined_table_name_part1}: {e}", "", 0, 0


def delete_partition(
    table_name_part1, partition, source_database, partitions_dict, 
    beeline_str, batch_id, insertion_timestamp, is_part2_table=False
):
    """
    Delete a partition from either the part1 or part2 table.

    Args:
        table_name_part1 (str): The base table name (without database prefix).
        partition (str): The partition specification.
        source_database (str): Source database name.
        partitions_dict (dict): Dictionary of partition key-value pairs.
        beeline_str (str): Beeline connection string.
        batch_id (str): The batch ID for logging.
        insertion_timestamp (str): Timestamp used as a partition in part2.
        is_part2_table (bool): If True, deletes from the part2 table instead of the part1 table.

    Returns:
        bool: True if deletion succeeded, False otherwise.
    """
    try:
        table_name_part2 = table_name_part1 + "_part2"
        joined_table_name_part1 = f"{source_database}.{table_name_part1}"
        joined_table_name_part2 = f"{source_database}.{table_name_part2}"
        table_to_delete = joined_table_name_part1

        partitions_dict_part2 = partitions_dict.copy()

        if is_part2_table:
            partitions_dict_part2["part2_insertion_time_partition"] = insertion_timestamp
            table_to_delete = joined_table_name_part2

        delete_rows_sql = format_delete_partition_query(table_to_delete, partitions_dict_part2)
        delete_new_rows_result = execute_beeline_sql(delete_rows_sql, beeline_str)

        if not delete_new_rows_result:
            logging.info(f"Delete operation failed from table: {table_to_delete}")
            print(f"Deleting operation failed from table: {table_to_delete}")
            logging.info(f"Manual delete operation command: {delete_rows_sql}")
            print(f"Manual delete operation command: {delete_rows_sql}")
            return False
        
        return True

    except Exception as e:
        print(f"Error while processing table {table_name_part1}: {e}")
        logging.error(f"Error while processing table {table_name_part1}: {e}")
        return False


def process_partitions(table_names, source_database, beeline_str, batch_id, insertion_timestamp):
    """
    Process a list of tables by identifying old partitions, archiving them to part2 tables,
    and removing them from the original table upon success.

    Args:
        table_names (list[str]): A list of table names to process.
        source_database (str): The source database containing the tables.
        beeline_str (str): Beeline connection string.
        batch_id (str): A unique batch identifier for this archival run.
        insertion_timestamp (str): Timestamp used for the part2 insertion partition.

    Returns:
        dict: A dictionary where keys are table names and values are dictionaries of partition metadata and statuses.
    """
    table_partitions_dict = {}

    for table_name in table_names:
        try:
            print("\n---------------------------------")
            print(f"Checking table: {table_name}")
            logging.info(f"Checking table: {table_name}")

            # If table name includes db name, override source_database
            if "." in table_name:
                source_database = table_name.split(".")[0]
                table_name = table_name.split(".")[-1]

            table_partitions_dict[table_name] = {}
            schema_lines = get_partitions_names(source_database, table_name, beeline_str)
            current_yr = datetime.datetime.now().year
            partitions_to_archive = []

            # Identify partitions to archive
            for schema in schema_lines:
                partition = schema.replace("|", "").strip()
                if not partition:
                    logging.warning("Empty partition found. Skipping.")
                    continue

                print(f"Checking partition: {partition}")
                logging.info(f"Checking partition: {partition}")

                partitions_dict = dict(item.split("=") for item in partition.split("/"))
                splited_list = partition.split("/")
                splited_parts_yr = []

                # Extract year from partition paths
                for part in splited_list:
                    temp_yr = part.split("=")[-1][:4]
                    if len(temp_yr) == 4 and temp_yr.isdigit():
                        splited_parts_yr.append(int(temp_yr))

                # If last year is older than 7 years and not 1970, consider archiving
                if len(splited_parts_yr) > 0:
                    last_yr = splited_parts_yr[-1]
                    yrs_diff = current_yr - last_yr
                    if yrs_diff > 7:
                        if last_yr != 1970:
                            logging.info(f"Adding partition to candidate list: {partition}")
                            print(f"Adding partition to candidate list: {partition}")
                            partitions_to_archive.append(partition)

                            table_partitions_dict[table_name][partition] = {
                                "partitions_dict_part1": partitions_dict,
                                "is_inserted_to_part2": False,
                                "is_deleted_from_part1": False,
                                "rollback_required": False,
                                "rollback_success": False,
                                "partitions_dict_part2": partitions_dict.copy(),
                            }
                        else:
                            print(f"Skipping partition: {partition}")
                            logging.info(f"Skipping partition: {partition}")
                    else:
                        print(f"Skipping partition: {partition}")
                        logging.info(f"Skipping partition: {partition}")

            # If we have partitions to archive, create part2 table and start archiving
            if len(partitions_to_archive) > 0:
                table_name_part2 = table_name + "_part2"
                joined_table_name_part1 = f"{source_database}.{table_name}"
                joined_table_name_part2 = f"{source_database}.{table_name_part2}"

                logging.info(f"Creating part2 table: {table_name_part2}")
                print(f"Creating part2 table: {table_name_part2}")

                create_sql_result, part1_column_names = create_part2_table(
                    joined_table_name_part1, joined_table_name_part2, beeline_str
                )

                if not create_sql_result:
                    create_failed_cmts = (
                        f"Failed to create part2 table: {joined_table_name_part2}. "
                        f"Table {joined_table_name_part1} remains as is."
                    )
                    print(create_failed_cmts)
                    logging.info(create_failed_cmts)
                    continue

                logging.info(f"Created part2 table: {joined_table_name_part2}")
                print(f"Created part2 table: {joined_table_name_part2}")

                total_part1_rows_to_archive = 0
                total_part2_rows_copied = 0
                failed_flag = False

                # Insert partitions into part2
                for partition in partitions_to_archive:
                    partitions_dict = table_partitions_dict[table_name][partition]["partitions_dict_part1"]
                    print(f"Adding data from '{partition}' to part2 table")
                    logging.info(f"Adding data from '{partition}' to part2 table")

                    res, comments, query, num_rows_partitions_part1, num_rows_after_insert_part2 = insert_to_part2_table(
                        table_name, partition, source_database, partitions_dict, beeline_str,
                        insertion_timestamp, part1_column_names
                    )

                    if res:
                        print(f"Successfully archived partition: {partition}")
                        logging.info(f"Successfully archived partition: {partition}")

                        total_part1_rows_to_archive += num_rows_partitions_part1
                        total_part2_rows_copied += num_rows_after_insert_part2

                        table_partitions_dict[table_name][partition]["partitions_dict_part2"]["part2_insertion_time_partition"] = insertion_timestamp
                        table_partitions_dict[table_name][partition]["is_inserted_to_part2"] = True
                    else:
                        print(f"Error archiving partition: {partition}. Rollback operation for part2.")
                        logging.info(f"Error archiving partition: {partition}. Rollback operation for part2.")
                        table_partitions_dict[table_name][partition]["is_inserted_to_part2"] = False
                        failed_flag = True

                # If all rows copied successfully, delete from part1
                if total_part1_rows_to_archive == total_part2_rows_copied and not failed_flag:
                    print("Successfully copied all partitions")
                    logging.info("Successfully copied all partitions")

                    for partition in partitions_to_archive:
                        if table_partitions_dict[table_name][partition]["is_inserted_to_part2"]:
                            res = delete_partition(
                                table_name, partition, source_database,
                                table_partitions_dict[table_name][partition]["partitions_dict_part1"], 
                                beeline_str, insertion_timestamp, insertion_timestamp, is_part2_table=False
                            )
                            table_partitions_dict[table_name][partition]["is_deleted_from_part1"] = res
                else:
                    # Rollback: delete from part2 if not everything copied
                    failed_flag = True
                    print("All rows did not copy successfully; we have to delete part2 partitions")
                    logging.info("All rows did not copy successfully; we have to delete part2 partitions")

                    for partition in partitions_to_archive:
                        partitions_dict_part2 = table_partitions_dict[table_name][partition]["partitions_dict_part2"]
                        res = delete_partition(
                            table_name, partition, source_database, partitions_dict_part2, 
                            beeline_str, insertion_timestamp, insertion_timestamp, is_part2_table=True
                        )
                        if res:
                            table_partitions_dict[table_name][partition]["rollback_success"] = True
                        else:
                            table_partitions_dict[table_name][partition]["rollback_required"] = True
                            table_partitions_dict[table_name][partition]["rollback_success"] = False

                # Log the operation results if no failure
                print("Logging operation in archival operation")
                logging.info("Logging operation in archival operation")

                if not failed_flag:
                    location = get_table_location(table_name_part2, source_database, beeline_str)
                    data_to_add_log = []

                    for partition in partitions_to_archive:
                        pdict_part1 = table_partitions_dict[table_name][partition]["partitions_dict_part1"]
                        pdict_part2 = table_partitions_dict[table_name][partition]["partitions_dict_part2"]

                        result_string_part1 = "/".join([f"{k}={v}" for k, v in pdict_part1.items()])
                        result_string_part2 = "/".join([f"{k}={v}" for k, v in pdict_part2.items()])

                        is_inserted_to_part2 = table_partitions_dict[table_name][partition]["is_inserted_to_part2"]
                        is_deleted_from_part1 = table_partitions_dict[table_name][partition]["is_deleted_from_part1"]
                        rollback_required = table_partitions_dict[table_name][partition]["rollback_required"]
                        rollback_success = table_partitions_dict[table_name][partition]["rollback_success"]

                        data_to_add_log.append((
                            table_name, table_name_part2, source_database, result_string_part1,
                            result_string_part2, current_datetime, insertion_timestamp, failed_flag, location,
                            is_inserted_to_part2, is_deleted_from_part1, rollback_required, rollback_success
                        ))

                    insert_log_operation_res = execute_beeline_sql(
                        format_batch_insert_log_query(source_database, data_to_add_log), beeline_str
                    )

                    if not insert_log_operation_res:
                        logging.info("Logged operation failed in archival_operation table")
                        print("Logged operation failed in archival_operation")
                    else:
                        logging.info("Logged operation in archival_operation table")
                        print("Logged operation in archival_operation")

        except Exception as e:
            print(f"Error while processing table {table_name}: {e}")
            logging.error(f"Error while processing table {table_name}: {e}")

    return table_partitions_dict


def main(include_file, source_database, beeline_str, to_email_ids, cc_email_ids, sender_email_id):
    """
    The main entry point of the archival script. It:
    - Generates a batch ID
    - Creates/Ensures the archival operation log table exists
    - Reads tables from an input file
    - Processes partitions for archival
    - Summarizes results
    - Sends an email notification with or without an attachment (CSV) based on results

    Args:
        include_file (str): Path to the file containing tables to process.
        source_database (str): The source database name.
        beeline_str (str): The beeline connection string.
        to_email_ids (str): Comma-separated list of recipient email IDs.
        cc_email_ids (str): Comma-separated list of CC email IDs.
        sender_email_id (str): The sender email ID.

    Returns:
        None
    """
    logging.info("Creating batch ID")
    print("Creating batch ID")

    insertion_timestamp = current_datetime_str
    batch_id = f"{generate_unique_id()}_{current_datetime_str}"
    logging.info(f"Created batch ID: {batch_id}")
    print(f"Created batch ID: {batch_id}")

    # Create the archival operation table if needed
    create_archival_op_query = format_create_archival_operation_table_query(source_database)
    logging.info("Creating archival operation log table")
    print("Creating archival operation log table")
    execute_beeline_sql(create_archival_op_query, beeline_str)
    logging.info("Created archival operation log table")
    print("Created archival operation log table")

    # Read tables from include_file
    with open(include_file, "r") as file:
        table_names = [line.strip() for line in file]

    # Process partitions for all tables
    table_partitions_dict = process_partitions(table_names, source_database, beeline_str, batch_id, insertion_timestamp)

    # Summarize results and log/print
    summary = summarize_table_partitions_dict(table_partitions_dict)
    print(summary)
    logging.info(summary)

    # Generate CSV if required
    csv_file = execute_beeline_csv(beeline_str, source_database, batch_id)
    start_date = datetime.datetime.today().strftime("%m-%d-%Y")

    logging.info("Sending mail")
    print("Sending mail")

    # If no CSV, send a summary email; otherwise, attach CSV
    if not csv_file:
        send_summary_email(batch_id, start_date, to_email_ids, summary, cc_email_ids, sender_email_id)
    else:
        send_email_notification(start_date, to_email_ids, csv_file, summary, cc_email_ids, sender_email_id)
        logging.info("Mail sent")
        print("Mail sent")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--include_file", required=True, help="File with tables to include")
    parser.add_argument("--source_database", required=True, help="Source database name")
    parser.add_argument("--beeline_str", required=True, help="Beeline connection string")
    parser.add_argument("--to_email_ids", required=True, help="Comma separated list of TO email ids")
    parser.add_argument("--cc_email_ids", required=False, default="", help="Comma separated list of CC email ids")
    parser.add_argument("--sender_email_id", required=False, default="", help="Sender email id")

    args = parser.parse_args()
    main(args.include_file, args.source_database, args.beeline_str, args.to_email_ids, args.cc_email_ids, args.sender_email_id)
