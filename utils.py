import subprocess
import datetime
import logging
from format_query_helper import *
from email_helper import *


# ------------------------------------------------------
# Beeline Execution Helper Functions
# ------------------------------------------------------
def execute_beeline_sql(query, beeline_str):
    """
    Execute a Hive query using beeline and return True if successful.

    Args:
        query (str): The Hive query to be executed.
        beeline_str (str): The beeline connection string.

    Returns:
        bool: True if query execution is successful, else False.
    """
    try:
        cmd = (
            """beeline -u "{}" --outputformat=csv2 --silent=true --verbose=false """
            """--showheader=true -e "set hive.exec.dynamic.partition.mode=nonstrict;" """
            """-e "set hive.tez.java.opts=-XX:+UseG1GC;" -e "{}" """.format(beeline_str, query)
        )
        subprocess.check_call(cmd, shell=True)
        return True

    except subprocess.CalledProcessError:
        return False


def execute_getdata_dsv_beeline_sql(query, beeline_str):
    """
    Execute a Hive query using beeline with DSV output format and return the result.

    Args:
        query (str): The Hive query to be executed.
        beeline_str (str): The beeline connection string.

    Returns:
        bytes or None: The raw output from beeline if successful, else None.
    """
    try:
        cmd = """beeline -u "%s" --outputformat=dsv --silent=true --verbose=false --showheader=false -e "%s" """ % (beeline_str, query)
        res = subprocess.check_output(cmd, shell=True)
        return res
    except subprocess.CalledProcessError as e:
        print("Error executing beeline: {}".format(e))
        return None


def execute_beeline_csv(beeline_str, source_database, batch_id):
    """
    Execute a Hive query using beeline with CSV output format and export the result to a CSV file.

    Args:
        beeline_str (str): The beeline connection string.
        source_database (str): The source database name.
        batch_id (str): The batch ID for filtering the operation log.

    Returns:
        str or None: The filename of the generated CSV if successful, else None.
    """
    try:
        current_date = datetime.datetime.today().strftime("%m-%d-%Y")
        cmd = """beeline -u "%s" --outputformat=csv2 --silent=true --verbose=false --showheader=true -e "SELECT * 
        FROM %s.archival_operation_log where batch_id='%s'" > pharmacy_tables_archival_log_%s.csv""" % (
            beeline_str, source_database, batch_id, current_date
        )
        subprocess.check_output(cmd, shell=True)
        return "pharmacy_tables_archival_log_%s.csv" % (current_date)
    except subprocess.CalledProcessError as e:
        print("execute_getdata_beeline_sql: Error executing beeline: {}".format(e))
        return None


def execute_getdata_beeline_sql(query, beeline_str):
    """
    Execute a Hive query using beeline with CSV2 output format and return the raw result.

    Args:
        query (str): The Hive query to be executed.
        beeline_str (str): The beeline connection string.

    Returns:
        bytes or None: The raw output from beeline if successful, else None.
    """
    try:
        cmd = """beeline -u "%s" --outputformat=csv2 --silent=true --verbose=false --showheader=false -e "%s" """ % (
            beeline_str, query
        )
        res = subprocess.check_output(cmd, shell=True)
        return res
    except subprocess.CalledProcessError as e:
        print("execute_getdata_beeline_sql: Error executing beeline: {}".format(e))
        return None


# ------------------------------------------------------
# Query Condition Helpers
# ------------------------------------------------------
def get_partition_condition(partitions_dict):
    """
    Generate a SQL WHERE clause condition for given partitions.

    Args:
        partitions_dict (dict): A dictionary of partition column-value pairs.

    Returns:
        str: A string representing the partition condition in SQL WHERE clause format.
    """
    condition = " AND ".join(["{} = '{}'".format(key, value) for key, value in partitions_dict.items()])
    return condition


# ------------------------------------------------------
# Data Retrieval and Processing Functions
# ------------------------------------------------------
def get_partitions_names(source_database, table_name, beeline_str):
    """
    Retrieve the partition names for a given table.

    Args:
        source_database (str): The database name.
        table_name (str): The table name.
        beeline_str (str): The beeline connection string.

    Returns:
        list[str]: A list of partition names.
    """
    query = format_get_partitions_query(source_database, table_name)
    result = execute_getdata_beeline_sql(query, beeline_str)
    if result is None:
        return []

    schema_lines = result.decode('utf-8').split('\n')
    partition_names = [line.strip() for line in schema_lines if line.strip()]
    return partition_names


def get_columns_excluding_partitions_beeline(table_name, partitions_dict, beeline_str):
    """
    Retrieve all columns from a table excluding those that are partition columns.

    Args:
        table_name (str): The name of the table.
        partitions_dict (dict): A dictionary of partition column-value pairs.
        beeline_str (str): The beeline connection string.

    Returns:
        list[str]: A list of column names excluding partition columns.
    """
    describe_query = format_describe_query(table_name)
    partitions = [key for key, value in partitions_dict.items()]

    result = execute_getdata_beeline_sql(describe_query, beeline_str)
    if result is None:
        return []

    output = result.decode('utf-8').split('\n')
    all_columns = [
        line.split("|")[1].strip() for line in output
        if line and 'col_name' not in line and '#' not in line and '--' not in line and
        len(line.split("|")) > 1 and line.split("|")[1].strip() != ""
    ]
    columns_excluding_partitions = [col for col in all_columns if col not in partitions]

    return columns_excluding_partitions


def create_part2_table(joined_table_name_part1, joined_table_name_part2, beeline_str):
    """
    Create a new external table (part2) based on the schema of an existing table (part1),
    adding an additional partition column 'part2_insertion_time_partition'.

    Args:
        joined_table_name_part1 (str): The name of the source table (part1).
        joined_table_name_part2 (str): The name of the target table (part2).
        beeline_str (str): The beeline connection string.

    Returns:
        tuple:
            bool: True if creation is successful, else False.
            list[str]: A list of column names for the created table.
    """
    try:
        res = execute_getdata_dsv_beeline_sql(format_get_create_query(joined_table_name_part1), beeline_str)
        if res is None:
            return False, []

        lines_data = []
        columns_data = []

        for line in res.encode("ascii", "ignore").decode("utf-8").split('\n'):
            if "ROW FORMAT SERDE" in line:
                break
            lines_data.append(line.strip().replace("\"", ""))
            if "PARTITIONED BY" not in line and "CREATE TABLE" not in line:
                columns_res = line.split()
                if len(columns_res) > 1:
                    columns_data.append(columns_res[1].strip())

        lines_data[0] = "CREATE EXTERNAL TABLE IF NOT EXISTS {} (".format(joined_table_name_part2)
        lines_data[-1] = lines_data[-1].replace(")", ", part2_insertion_time_partition string)")

        create_stmt_str = " ".join(lines_data)
        execute_beeline_sql(create_stmt_str, beeline_str)
        return True, columns_data

    except Exception as e:
        print("create_part2_table: Error while creating part2 table: {}".format(e))
        return False, []


def get_row_count(joined_table_name, beeline_str, condition=None):
    """
    Get the count of rows from a table possibly filtered by a given condition.

    Args:
        joined_table_name (str): The name of the table.
        beeline_str (str): The beeline connection string.
        condition (str, optional): A WHERE clause condition to filter rows. Defaults to None.

    Returns:
        int or None: The number of rows if successful and count is numeric, else None.
    """
    try:
        query = format_get_count_query(joined_table_name, where=condition)
        result = execute_getdata_beeline_sql(query, beeline_str)
        if result is None:
            print("Aborting operation as get rows count failed")
            return None

        num_rows = result.strip()
        if num_rows and num_rows.isdigit():
            return int(num_rows)
        else:
            print("Aborting operation as get rows count failed")
            return None
    except Exception as e:
        print("Error getting row count: {}".format(e))
        return None


def get_table_location(table_name, source_database, beeline_str):
    """
    Get the HDFS location of a given table in a specified database.

    Args:
        table_name (str): The name of the table.
        source_database (str): The database name.
        beeline_str (str): The beeline connection string.

    Returns:
        str or None: The table location URI if found, else None.
    """
    try:
        cmd = """beeline -u "%s" --outputformat=csv2 --silent=true --verbose=false --showheader=false -e "USE %s; DESCRIBE FORMATTED %s" """ % (
            beeline_str, source_database, table_name
        )
        output = subprocess.check_output(cmd, shell=True)
        for line in output.splitlines():
            if line.strip().startswith("Location"):
                return line.split(":")[1].strip()
        return None
    except Exception as e:
        print("Exception occurred while getting location of table: {}".format(table_name))
        print(e)
        return None


# ------------------------------------------------------
# Summarization and Reporting
# ------------------------------------------------------
def summarize_table_partitions_dict(table_partitions_dict):
    """
    Generate a summary of partitions operations (insertions, deletions, rollback) for multiple tables.

    Args:
        table_partitions_dict (dict):
            A dictionary of the form:
            {
                "table_name": {
                    "partition_key_value": {
                        "is_inserted_to_part2": bool,
                        "is_deleted_from_part1": bool,
                        "rollback_required": bool,
                        "rollback_success": bool
                    },
                    ...
                },
                ...
            }

    Returns:
        tuple:
            list[str]: A list of summary lines for each table.
            str: An overall summary line for all tables combined.
    """
    summary = []
    total_tables = len(table_partitions_dict)
    total_partitions_inserted_to_part2 = 0
    total_partitions_deleted_from_part1 = 0
    total_rollback_required = 0
    total_rollback_success = 0

    for table_name, partitions in table_partitions_dict.items():
        inserted_to_part2 = sum(1 for p in partitions.values() if p["is_inserted_to_part2"])
        deleted_from_part1 = sum(1 for p in partitions.values() if p["is_deleted_from_part1"])
        rollback_required = sum(1 for p in partitions.values() if p["rollback_required"])
        rollback_success = sum(1 for p in partitions.values() if p["rollback_success"])

        total_partitions_inserted_to_part2 += inserted_to_part2
        total_partitions_deleted_from_part1 += deleted_from_part1
        total_rollback_required += rollback_required
        total_rollback_success += rollback_success

        summary.append(
            "\nTable: {}: Partitions: Inserted to Part2: {}, Deleted from Part1: {}, Rollback Required: {}, Rollback Success: {}".format(
                table_name, inserted_to_part2, deleted_from_part1, rollback_required, rollback_success
            )
        )

    overall_summary = (
        "Total Tables: {}, Total Partitions Inserted to Part2: {}, Total Partitions Deleted from Part1: {}, "
        "Total Rollback Required: {}, Total Rollback Success: {}".format(
            total_tables, total_partitions_inserted_to_part2, total_partitions_deleted_from_part1,
            total_rollback_required, total_rollback_success
        )
    )

    return summary, overall_summary
