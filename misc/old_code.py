def call_flights_endpoint(flight_date=None, departure_airport_iata=None, arrival_airport_iata=None, flight_iata=None, limit=100, offset=0):
    """
    Calls the flights API endpoint from the Aviationstack API.

    Parameters:
        flight_date (str): date of the flight data to be retrieved
        departure_airport_iata (str): the IATA code of the departure airport
        arrival_airport_iata (str): the IATA code of the arrival airport
        flight_iata (str): the IATA code of the flight
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
    
    # add flight number
    if flight_iata is not None:
        req_url += f'&flight_iata={flight_iata}'
    
    # add flight number
    if flight_date is not None:
        req_url += f'&flight_date={flight_date}'

    logger.info(f'Calling API with URL: {req_url}')

    with urllib.request.urlopen(req_url) as response:
        html = response.read()
        api_resp = html.decode(encoding='utf-8')

    return api_resp


def call_timetable_endpoint(iataCode=None, type=None, limit=100, offset=0):
    """
    Calls the timetable API endpoint from the Aviationstack API. This returns
    live airport schedule data at the time of the API call.

    Parameters:
        iataCode (str): the IATA code of the airport
        type (str): either 'departure' or 'arrival'
        limit (int): limit of records to fetch (100 is the max for Aviationstack per API call)
        offset (int): offset used for pagination

    Returns:
        str: the API response
    """

    # get base URL with limit and offset
    req_url = form_aviationstack_api_call(api_route='timetable', api_key=AVIATIONSTACK_API_KEY, limit=limit, offset=offset)

    # add arguments to the API call URL
    req_url += f'&iataCode={iataCode}'
    req_url += f'&type={type}'

    logger.info(f'Calling API with URL: {req_url}')

    with urllib.request.urlopen(req_url) as response:
        html = response.read()
        api_resp = html.decode(encoding='utf-8')

    return api_resp


def get_flights(max_pages=3, flight_date=None, departure_airport_iata=None, arrival_airport_iata=None, flight_iata=None):
    """
    Gets live or historical flight data from the 'flights' route of the
    Aviationstack API for a given departure and/or arrival airport.
    This function is a wrapper around call_flights_endpoint() that iterates
    through all the pages available until all data is retrieved.

    Parameters:
        max_pages (int): maximum number of pages to get from the API. -1 fetches all pages.
        flight_date (str): date of the flight data to be retrieved
        departure_airport_iata (str): the IATA code of the departure airport
        arrival_airport_iata (str): the IATA code of the arrival airport
        flight_iata (str): the IATA code of the flight

    Returns:
        str: all pages of data, formatted as a nested JSON
    """

    if departure_airport_iata is None and arrival_airport_iata is None:
        raise Exception('No departure or arrival airport specified.')

    logger.info(f'Function was called with args: get_flights(max_pages={max_pages}, flight_date={flight_date}, departure_airport_iata={departure_airport_iata}, arrival_airport_iata={arrival_airport_iata}, flight_iata={flight_iata})')

    # API max limit = 100
    # starting offset
    limit = 100 
    offset = 0

    logger.info(f'Fetching first {limit} records starting at offset {offset}.')

    # fetch the first batch
    api_resp = call_flights_endpoint(flight_date=flight_date,
                                     departure_airport_iata=departure_airport_iata,
                                     arrival_airport_iata=arrival_airport_iata,
                                     flight_iata=flight_iata,
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
        logger.info(f"Fetching {limit} records starting at offset {offset}. Page: {int((offset / limit) + 1)} of max pages: {max_pages}.")
        api_resp = call_flights_endpoint(flight_date=flight_date,
                                         departure_airport_iata=departure_airport_iata,
                                         arrival_airport_iata=arrival_airport_iata,
                                         flight_iata=flight_iata,
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

def get_timetable(max_pages=3, iataCode=None, type=None):
    """
    Gets live timetable from the 'timetable' route of the
    Aviationstack API for a given airport. Must choose either
    departures or arrivals. 
    
    This function is a wrapper around call_timetable_endpoint() that iterates
    through all the pages available until all data is retrieved.

    Parameters:
        max_pages (int): maximum number of pages to get from the API. -1 fetches all pages.
        iataCode (str): the IATA code of the airport
        type (str): either 'departure' or 'arrival'

    Returns:
        str: all pages of data, formatted as a nested JSON
    """

    if iataCode is None or type is None:
        raise Exception("Must specify iataCode and type ('departure' or 'arrival').")
    
    logger.info(f'Function was called with args: get_timetable(max_pages={max_pages}, iataCode={iataCode}, type={type})')

    # API max limit = 100
    # starting offset
    limit = 100 
    offset = 0

    logger.info(f'Fetching first {limit} records starting at offset {offset}.')

    # fetch the first batch
    api_resp = call_timetable_endpoint(iataCode=iataCode,
                                       type=type,
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
        # must sleep > 60 seconds
        # API rate limit: 1 call per minute
        logger.info('Sleeping for 65 seconds due to API rate limit.')
        time.sleep(65)

        logger.info(f"Fetching {limit} records starting at offset {offset}. Page: {int((offset / limit) + 1)} of max pages: {max_pages}.")
        api_resp = call_timetable_endpoint(iataCode=iataCode,
                                           type=type,
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


# # get flight data
flight_data = get_flights(max_pages=5,
                          flight_date='2025-03-01',
                          departure_airport_iata='MKE',
                          #flight_iata='WN1304'
                          )

flight_data = get_timetable(max_pages=3,
                            iataCode='MKE',
                            type='departure')
