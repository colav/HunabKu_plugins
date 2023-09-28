from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from kamunu import kamunu_main, id_input
from hunabku.Config import Config, Param
from pymongo import MongoClient
from flask import request
import re


class Kamunu(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")

    config += Param(db_name="organizations_ids",
                    doc="Mongo DB name")

    config += Param(records_collection="records_collection",
                    doc="Mongo DB collection")

    config += Param(not_inserted_collection="not_inserted",
                    doc="Mongo DB collection")

    config += Param(apikey="colavudea",
                    doc="Plugin API key")

    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.db_uri)
        self.db = self.dbclient[self.config.db_name]
        self.records_collection = self.db[self.config.records_collection]
        self.not_inserted_collection = self.db[self.config.not_inserted_collection]
        self.apikey = self.config.apikey

    @endpoint('/organizations', methods=['GET'])
    def search_organizations(self):
        """
        @api {get} /organizations Organizations IDs finder
        @apiName Organizations finder plugin
        @apiGroup Oganizations
        @apiDescription Allows to perform searches for information about organizations

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} query Organization name or identifier
        @apiParam {String} country Country of the organization (Optional)

        @apiParam {String="IDs_Only", "Dehydrated_document" ,"Full_document", "Custom"} return="Dehydrated_document" Options for search response
        @apiParam {String="_id", "raw_name" ,"names", "ids", "categories", "location", "records"} key="location" Options for custom key
        @apiParam {String} source Source of the organization name (Optional)

        @apiSuccess Document/Dict Dehydrated document of the organization

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.
        @apiError (Error 404) msg  Not Found, there were no valid results for the organization.

        @apiExample {curl} Example usage:
            # Get dehydrated document of the organization.
            curl -i http://apis.colav.co/organizations?apikey=XXXX&query=Universidad%20de%20antioquia&return=Dehydrated_document

            # Get only organization identifiers.
            curl -i http://apis.colav.co/organizations?apikey=XXXX&query=Universidad%20de%20antioquia&return=IDs_Only

            # Get a specific key of the document. ('_id', 'raw_name', 'names', 'ids', 'categories', 'location', 'records')
            curl -i http://apis.colav.co/organizations?apikey=XXXX&query=Universidad%20de%20antioquia&return=custom&key=location

        """

        if not self.valid_apikey():
            return self.apikey_error()

        query = request.args.get('query')
        source = request.args.get('source')
        return_ = request.args.get('return')
        key = request.args.get('key')
        country = request.args.get('country')

        def bd_search(key: str, query: str):
            """"
            Search for the organization in the collection according to the ID

            Arg:
                key (str): The key according to the ID
                query (str): The input Id string to be find.

            Returns:
                The record found.
            """

            results = None
            # Search for the organization in the collection
            results = self.records_collection.find_one({key: query})

            return results

        def detect_input_type(query):
            """
            Detects the type of input based on different patterns.

            Args:
                query (str): The input string to be detected.

            Returns:
                str: The type of input detected.
            """
            # Regular expression patterns for different types of inputs
            ror_url_pattern = r'^https:\/\/ror\.org\/\w+$'
            wikidata_url_pattern = r'^https:\/\/www\.wikidata\.org\/wiki\/Q\d+$'
            wikidata_id_pattern = r'^Q\d+$'
            ror_id_pattern = r'^\w+$'

            # Checking patterns
            if re.match(ror_url_pattern, query):
                return bd_search('ids.ror', query)
            elif re.match(wikidata_url_pattern, query):
                return bd_search('ids.wikidata', query)
            elif re.match(wikidata_id_pattern, query):
                return bd_search('ids.wikidata', "https://www.wikidata.org/wiki/" + query)
            elif re.match(ror_id_pattern, query) and len(query) == 9 and query.startswith('0') and query[-1].isdigit():
                return bd_search('ids.ror', "https://ror.org/" + query)
            else:
                return 'string'

        query = request.args.get('query')
        if query:
            input = detect_input_type(query)

            if country:
                country = country.lower().title()

            if input == 'string' and source:
                kamunu_query = query
                kamunu_source = source
                insert = kamunu_main.single_organization(
                    kamunu_query, kamunu_source, country)
                response = bd_search('_id', insert['_id'])

            elif input == 'string':
                kamunu_query = query
                insert = kamunu_main.single_organization(
                    kamunu_query, "single_search", country)
                if insert:
                    response = bd_search('_id', insert['_id'])
                else:
                    response = None

            else:
                if input is None:
                    if source:
                        kamunu_source = source
                    else:
                        kamunu_source = "single_search"

                    insert = id_input.id_as_input(
                        query, kamunu_source, country)
                    response = bd_search('_id', insert['_id'])
                else:
                    response = input

            if not return_:
                data = {
                    "message": "It is necessary to define the 'return' parameter"}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response

            if response:
                if return_.lower() == "ids_only" or return_.lower() == "only_ids" or return_.lower() == "ids":
                    return self.app.response_class(
                        response=self.json.dumps(response['ids'], default=str),
                        status=200,
                        mimetype='application/json'
                    )

                elif return_.lower() == "dehydrated_document" or return_.lower() == "dehydrated":
                    response.pop('records')
                    response.pop('validation')
                    return self.app.response_class(
                        response=self.json.dumps(response, default=str),
                        status=200,
                        mimetype='application/json'
                    )

                elif return_.lower() == "full_document" or return_.lower() == "full":
                    response.pop('validation')
                    return self.app.response_class(
                        response=self.json.dumps(response, default=str),
                        status=200,
                        mimetype='application/json'
                    )

                elif return_.lower() == "custom":
                    # Custom key
                    if not key:
                        key = 'location'
                    key = key.lower()
                    if key not in ['_id', 'raw_name', 'names', 'ids', 'categories', 'location', 'records']:
                        return self.app.response_class(
                            response=self.json.dumps(
                                {'message': 'Invalid key'}),
                            status=400,
                            mimetype='application/json'
                        )
                    else:
                        return self.app.response_class(
                            response=self.json.dumps(
                                response[f'{key}'], default=str),
                            status=200,
                            mimetype='application/json'
                        )
                else:
                    data = {"message": "Invalid value for 'return' parameter"}
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=400,
                        mimetype='application/json'
                    )
                    return response

            else:
                if country:
                    message = {
                        'message': f'Not Found: There were no valid results for the organization: {query} - {country}'}
                else:
                    message = {
                        'message': f'Not Found: There were no valid results for the organization: {query}'}

                return self.app.response_class(
                    response=self.json.dumps(message),
                    status=404,
                    mimetype='application/json'
                )
