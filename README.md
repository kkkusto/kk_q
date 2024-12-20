import datetime
import random
import string

def missing_from_second_list_with_same_prefix(list1, list2):
    """
    Return a dictionary mapping original missing filenames to their new 
    prefixed names, where prefix = todays_datetime_<5charID>_.
    All missing files share the same ID.
    """
    # Identify missing files
    set_list2 = set(list2)
    missing_files = [f for f in list1 if f not in set_list2]

    # If there are no missing files, return empty dict
    if not missing_files:
        return {}

    # Get current date/time in a formatted string
    now = datetime.datetime.now()
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")

    # Generate a single 5-character ID
    chars = string.ascii_letters + string.digits
    rand_id = ''.join(random.choice(chars) for _ in range(5))

    # Create a new name for each missing file using the single generated ID
    renamed = {}
    for missing_file in missing_files:
        new_name = "{}_{}_{}".format(timestamp_str, rand_id, missing_file)
        renamed[missing_file] = new_name

    return renamed

# Example usage:
files_in_source = ["file1.txt", "file2.txt", "file3.txt", "file4.png"]
files_in_dest = ["file2.txt", "file4.png"]

renamed_missing = missing_from_second_list_with_same_prefix(files_in_source, files_in_dest)
print(renamed_missing)
# Example output (keys are original missing files, values are renamed):
# {
#   'file1.txt': '20241220_153045_a2ZrX_file1.txt',
#   'file3.txt': '20241220_153045_a2ZrX_file3.txt'
# }
