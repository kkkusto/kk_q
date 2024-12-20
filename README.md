def missing_from_second_list(list1, list2):
    """
    Return a list of filenames that are present in list1 but not in list2.
    """
    set_list2 = set(list2)
    missing_files = [f for f in list1 if f not in set_list2]
    return missing_files

# Example usage:
files_in_source = ["file1.txt", "file2.txt", "file3.txt", "file4.png"]
files_in_dest = ["file2.txt", "file4.png"]

missing = missing_from_second_list(files_in_source, files_in_dest)
print(missing)  # Output: ['file1.txt', 'file3.txt']
