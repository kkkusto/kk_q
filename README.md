import pandas as pd
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO
# Create an example DataFrame for df
example_data_1 = '''
extracted_name
val1/subscriptions/val2
val3/subscriptions/val4
'''
df = pd.read_csv(StringIO(example_data_1))
def extract_name(row):
    source = row['source1'] if pd.notna(row['source1']) else row['source2']
    if pd.notna(source) and 'name:' in source:
        return source.split('name:')[1].split('|')[0]
    return ''

# Apply the function to the DataFrame
df['extracted_name'] = df.apply(extract_name, axis=1)
# Split the 'extracted_name' column
df[['column1', 'column2']] = df['extracted_name'].str.split('/subscriptions/', expand=True)
grouped_df = df.groupby('left_of_subscription').agg({'total': 'sum'}).reset_index()

print(grouped_df)
# Create an example DataFrame for new_df
example_data_2 = '''
new_col,data
val1,100
val3,200
'''
new_df = pd.read_csv(StringIO(example_data_2))

# Merge the two DataFrames
merged_df = pd.merge(grouped_df, new_df, left_on='column1', right_on='new_col', how='inner')

print(merged_df)
