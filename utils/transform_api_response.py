import json
import flatten_json
from datetime import datetime, timezone

def flatten_and_save_json(input_json):
    """
    Takes multiple pages of nested JSON as input, iterates through each page,
    unnests the JSON, and returns a string that can be saved as a .json file.

    Parameters:
        input_json (list): the input JSON object. This must first be deserialized with json.loads().

    Returns:
        str: The flattened JSON as a string.
    """

    # open with a [
    output_str = '['

    # iterate through all pages from the API response
    # unnest each page
    for page in range(len(input_json)):
        for record in input_json[page]['data']:
            flat = flatten_json.flatten(record)

            output_str += json.dumps(flat)

            # add comma after every record
            output_str += ','

    # drop the last comma
    output_str = output_str[:-1]
    
    # close with a ]
    output_str += ']'

    return output_str


def transform_flat_json_to_csv(flat_json):
    """
    Takes a flat JSON as input and returns the same data formatted as 
    a string that can be saved as a CSV file.

    Parameters:
        flat_json (list): the input JSON object. This must first be deserialized with json.loads().

    Returns:
        str: a string that can be saved as a CSV file.
    """

    # get list of all possible columns
    column_set = set()

    for record in flat_json:
        for key in record:
            column_set.add(key)
            
    # sort it
    column_set = sorted(column_set)

    # add the header
    header_str = ''
    output_str = ''
    for col in column_set:
        header_str += col + ','

    # add row_ts column
    header_str += 'row_ts'
    
    output_str = header_str + '\n'

    # populate the rows
    for record in flat_json:
        for col in column_set:
            # add the value in the correct position
            try:
                output_str += record[col] + ','
            # it the column is not in the response for this record, an exception will be thrown
            # fill it with null in the output CSV file
            except:
                output_str += ','
            
        # add current timestamp as a value in the row
        output_str += datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        # newline for every record
        output_str += '\n'
    
    return output_str
