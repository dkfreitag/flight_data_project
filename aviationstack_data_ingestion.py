from dotenv import load_dotenv
import os
import json
import urllib.request
import logging

from utils.transform_api_response import *

# set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='MKE_departures_short_20250301.log',
                    encoding='utf-8',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# load the .env file into the environment variables
load_dotenv()
AVIATIONSTACK_API_KEY = os.getenv("AVIATIONSTACK_API_KEY")


def form_aviationstack_api_call(api_route, api_key, limit, offset):
    """
    Forms the Aviationstack API call.

    Parameters:
        api_route (str): the API route (for example, 'flights')
        api_key (str): API key
        limit (int): limit of records to fetch (100 is the max for Aviationstack per API call)
        offset (int): offset used for pagination

    Returns:
        str: the API call string
    """

    req = f'https://api.aviationstack.com/v1/{api_route}?access_key={api_key}&limit={limit}&offset={offset}'
    return req


def call_flights_endpoint(departure_airport_iata=None, arrival_airport_iata=None, limit=100, offset=0):
    """
    Calls the flights API endpoint from the Aviationstack API.

    Parameters:
        departure_airport_iata (str): the IATA code of the departure airport
        limit (int): limit of records to fetch (100 is the max for Aviationstack per API call)
        offset (int): offset used for pagination

    Returns:
        str: the API response
    """

    # get base URL with limit and offset
    req_url = form_aviationstack_api_call(api_route='flights', api_key=AVIATIONSTACK_API_KEY, limit=limit, offset=offset)

    # add departure airport filter
    if departure_airport_iata is not None:
        req_url += f'&dep_iata={departure_airport_iata}'

    # add arrival airport filter
    if arrival_airport_iata is not None:
        req_url += f'&arr_iata={arrival_airport_iata}'

    logger.info(f'Calling API with URL: {req_url}')

    with urllib.request.urlopen(req_url) as response:
        html = response.read()
        api_resp = html.decode(encoding='utf-8')

    return api_resp


def get_live_flights(max_pages=3, departure_airport_iata=None, arrival_airport_iata=None):
    """
    Gets live flight data from the 'flights' route of the Aviationstack API for
    a given departure and/or arrival airport. This function iterates through
    all the pages available until all data is retrieved.

    Parameters:
        departure_airport_iata (str): the IATA code of the departure airport
        arrival_airport_iata (str): the IATA code of the arrival airport
        max_pages (int): maximum number of pages to get from the API. -1 fetches all pages.

    Returns:
        str: all pages of data, formatted as a nested JSON
    """

    if departure_airport_iata is None and arrival_airport_iata is None:
        raise Exception('No departure or arrival airport specified.')

    # API max limit = 100
    # starting offset
    limit = 100 
    offset = 0

    logger.info(f'Fetching first {limit} records starting at offset {offset}.')

    # fetch the first batch
    api_resp = call_flights_endpoint(departure_airport_iata=departure_airport_iata, arrival_airport_iata=arrival_airport_iata, limit=limit, offset=offset)

    # write out to a file
    # start with a [ character, since we will have multiple JSON records
    output_str = '['
    output_str += api_resp

    # convert to dictionary
    api_resp_dict = json.loads(api_resp)

    # increment offset
    offset += limit

    # if max_pages is set to -1, fetch all pages
    if max_pages == -1:
        max_pages = (api_resp_dict['pagination']['total'] // limit) + 1

    # how many pages are we going to get?
    # either all pages or max_pages, whichever is less
    max_pages = min((api_resp_dict['pagination']['total'] // limit) + 1, max_pages)

    logger.info(f'Total possible records that could be fetched: {api_resp_dict['pagination']['total']}')
    logger.info(f'Total possible pages that could be fetched: {(api_resp_dict['pagination']['total'] // limit) + 1}')
    logger.info(f'Max pages: {max_pages}')

    # while there are still results to fetch, fetch them and append to the file
    while (offset / limit) < max_pages:
        logger.info(f"Fetching {limit} records starting at offset {offset}. Page: {int((offset / limit) + 1)} of max pages: {max_pages}.")
        api_resp = call_flights_endpoint(departure_airport_iata=departure_airport_iata, arrival_airport_iata=arrival_airport_iata, limit=limit, offset=offset)

        # append out to a file
        # add a comma between records
        output_str += ','
        output_str += api_resp
    
        # increment offset
        offset += limit

        # reload api_resp_dict in case the total number of results changed
        api_resp_dict = json.loads(api_resp)
    
    # end a [ character, since we will have multiple JSON records
    output_str += ']'

    return output_str


def main():
    # filenames
    api_call_input_file = 'api_call_output.json'
    flat_json_filename = 'flat_output.json'
    final_csv_output_filename = 'MKE_departures_short_20250301.csv'


    # get flight data
    logger.info('Getting live flight data for departure airport MKE.')
    flight_data = get_live_flights(max_pages=3, departure_airport_iata='MKE')
    
    # logger.info('Getting live flight data for arrival airport MKE.')
    # flight_data = get_live_flights(arrival_airport_iata='MKE')


    # save it to a file just for fun
    logger.info(f'Done downloading data from the API. Saving to {api_call_input_file}')
    with open(api_call_input_file, 'w') as f:
        f.write(flight_data)

    # open the API call output and read it into a variable
    with open(api_call_input_file) as f:
        api_resp = f.read()

    # transform the API response into a flat JSON structure
    logger.info('Flattening the API response JSON.')
    flat_json = flatten_and_save_json(json.loads(api_resp))

    # transform the flat JSON into CSV format
    logger.info('Transforming the flat JSON to CSV format.')
    csv_output = transform_flat_json_to_csv(json.loads(flat_json))

    
    # save it as a .json file just for fun
    logger.info(f'Saving files: {flat_json_filename} and {final_csv_output_filename}')

    with open(flat_json_filename, 'w') as f:
        f.write(flat_json)

    # save the output as a CSV
    with open(final_csv_output_filename, 'w') as f:
        f.write(csv_output)


if __name__ == '__main__':
    main()
