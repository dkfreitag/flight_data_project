from dotenv import load_dotenv
import os
import json
import urllib.request
import logging
import time

from utils.transform_api_response import *

# set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='ingestion.log',
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


def get_pages_decorator(func):
    """
    Decorator that iterates through the API response pages.
    
    Globals used by this decorator:
        PAGE_LIMIT_GLOBAL: global variable defining maximum number of pages to fetch.
        PAGE_SLEEP_GLOBAL: global variable defining number of seconds to sleep between pages for rate limits.
    """
    def wrapper(*args, **kwargs):
        max_pages = PAGE_LIMIT_GLOBAL
        page_sleep_time = PAGE_SLEEP_GLOBAL

        logger.info(f'Function {func.__name__} was called with args: {kwargs}')
        logger.info(f'max_pages set to: {max_pages}')
        logger.info(f'page_sleep_time set to: {page_sleep_time}')

        # API max limit = 100
        # starting offset
        limit = 100 
        offset = 0

        logger.info(f'Fetching first {limit} records starting at offset {offset}.')

        # fetch the first batch
        api_resp = func(*args, **kwargs,
                        limit=limit,
                        offset=offset)

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
            if page_sleep_time > 0:
                logger.info(f"Sleeping for {page_sleep_time} seconds due to API call rate limits.")
                time.sleep(page_sleep_time)

            logger.info(f"Fetching {limit} records starting at offset {offset}. Page: {int((offset / limit) + 1)} of max pages: {max_pages}.")
            
            # fetch next batch
            api_resp = func(*args, **kwargs,
                            limit=limit,
                            offset=offset)

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
    
    return wrapper


@get_pages_decorator
def call_api_endpoint(api_route=None, flight_date=None, departure_airport_iata=None, arrival_airport_iata=None, flight_iata=None, iataCode=None, type=None, limit=100, offset=0):
    """
    Calls an API endpoint from the Aviationstack API.

    Parameters:
        api_route (str): API route to hit
        
        flights endpoint parameters:
            flight_date (str) - optional: date of the flight data to be retrieved
            departure_airport_iata (str): the IATA code of the departure airport - required if arrival_airport_iata not set
            arrival_airport_iata (str): the IATA code of the arrival airport - required if arrival_airport_iata not set
            flight_iata (str) - optional: the IATA code of the flight

        timetable endpoint parameters:
            iataCode (str) - required: the IATA code of the airport
            type (str) - required: either 'departure' or 'arrival'

        used by the decorator - not necessary to pass:
            limit (int): limit of records to fetch (100 is the max for Aviationstack per API call)
            offset (int): offset used for pagination

    Returns:
        str: the API response
    """

    # error handling
    if api_route is None:
        raise Exception('No API route specified.')
    
    if api_route == 'flights':
        if departure_airport_iata is None and arrival_airport_iata is None:
            raise Exception('No departure or arrival airport specified for the flights API route.')
    
    elif api_route == 'timetable':
        if iataCode is None or type is None:
            raise Exception("Must specify iataCode and type ('departure' or 'arrival') for the timetable API route.")
        if PAGE_SLEEP_GLOBAL < 62:
            raise Exception(f"PAGE_SLEEP_GLOBAL is {PAGE_SLEEP_GLOBAL}. It must be >= 62 seconds for the timetable API route.")


    # get base URL with limit and offset
    req_url = form_aviationstack_api_call(api_route=api_route, api_key=AVIATIONSTACK_API_KEY, limit=limit, offset=offset)


    # flights endpoint API parameters
    if flight_date is not None:
        req_url += f'&flight_date={flight_date}'

    if departure_airport_iata is not None:
        req_url += f'&dep_iata={departure_airport_iata}'

    if arrival_airport_iata is not None:
        req_url += f'&arr_iata={arrival_airport_iata}'

    if flight_iata is not None:
        req_url += f'&flight_iata={flight_iata}'
    

    # timetable endpoint API parameters
    req_url += f'&iataCode={iataCode}'
    req_url += f'&type={type}'


    logger.info(f'Calling API with URL: {req_url}')

    with urllib.request.urlopen(req_url) as response:
        html = response.read()
        api_resp = html.decode(encoding='utf-8')

    return api_resp


def main():
    # filenames
    api_call_input_file = 'api_call_output.json'
    flat_json_filename = 'flat_output.json'
    final_csv_output_filename = 'timetable_data.csv'

    global PAGE_LIMIT_GLOBAL
    global PAGE_SLEEP_GLOBAL

    PAGE_LIMIT_GLOBAL = 3
    PAGE_SLEEP_GLOBAL = 0

    # flight_data = call_api_endpoint(api_route='flights',
    #                                 flight_date='2025-03-01',
    #                                 departure_airport_iata='MKE')

    # flight_data = call_api_endpoint(api_route='flights',
    #                                 flight_date='2025-03-01',
    #                                 arrival_airport_iata='MKE')

    PAGE_SLEEP_GLOBAL = 65
    flight_data = call_api_endpoint(api_route='timetable',
                                    iataCode='MKE',
                                    type='departure')
    
    # flight_data = call_api_endpoint(api_route='timetable',
    #                                 iataCode='MKE',
    #                                 type='arrival')


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
