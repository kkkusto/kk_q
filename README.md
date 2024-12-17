def extract_enter_month_values(strings):
    results = []
    for s in strings:
        # Split the string on '/'
        parts = s.split('/')
        
        # Iterate over each part to find the one containing 'fill_enter_mnth='
        for part in parts:
            if part.startswith('fill_enter_mnth='):
                # Extract the value after '='
                value = part.split('=', 1)[1]
                results.append(value)
                break  # Proceed to the next string once found
    return results

# Example usage:
input_list = ['fill_sold_yr=2011/fill_enter_mnth=200012']
output = extract_enter_month_values(input_list)
print(output)  # Should print ['200012']
