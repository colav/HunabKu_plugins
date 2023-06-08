from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from pymongo import MongoClient
from flask import Response, request
import csv


class International(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")

    config += Param(db_name="international",
                    doc="Mongo DB name")

    config += Param(mobility_collection_name="international_mobility",
                    doc="Mongo DB collection")

    config += Param(agreements_collection_name="agreements_raw",
                    doc="Mongo DB collection")

    config += Param(apikey="colav",
                    doc="Plugin API key")

    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.db_uri)
        self.db = self.dbclient[self.config.db_name]
        self.mobility_collection = self.db[self.config.mobility_collection_name]
        self.agreements_collection = self.db[self.config.agreements_collection_name]
        self.apikey = self.config.apikey

    def valid_apikey(self):
        if self.request.method == 'POST':
            apikey = self.request.form.get('apikey')
        else:
            apikey = self.request.args.get('apikey')
        if self.apikey == apikey:
            return True
        else:
            return False

    @endpoint('/agreements', methods=['GET'])
    def get_agreements(self):
        """
        @api {get} /agreements Agreements records
        @apiName Agreements
        @apiGroup DRI UdeA

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String="json","csv"}[format='json'] Response format

        @apiSuccess agreements Agreements records in JSON or CSV format.

        """

        if not self.valid_apikey():
            return self.apikey_error()

        format_param = request.args.get('format', 'json')

        if format_param.lower() == 'csv':
            return self.get_agreements_csv()
        elif format_param.lower() == 'json':
            return self.get_agreements_json()
        else:
            return self.badrequest_error()

    def get_agreements_json(self):
        data = list(self.agreements_collection.find())
        response = self.app.response_class(
            response=self.json.dumps(data, default=str, ensure_ascii=False),
            status=200,
            mimetype='application/json'
        )
        return response

    def get_agreements_csv(self):
        data = list(self.agreements_collection.find())

        if data:
            keys = data[0].keys()

            response = Response(content_type='text/csv')
            response.headers.set('Content-Disposition',
                                 'attachment', filename='agreements.csv')

            writer = csv.DictWriter(response.stream, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

            return response
        else:
            return self.app.response_class(
                response=self.json.dumps({'message': 'No data found.'}),
                status=404,
                mimetype='application/json'
            )

    @endpoint('/mobility', methods=['GET'])
    def get_mobility(self):
        """
        @api {get} /mobility Mobility records
        @apiName Mobility
        @apiGroup DRI UdeA

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String="json","csv"} format="json" Response format


        @apiSuccess {Object|file} mobility International mobility records in JSON or CSV format.

        """

        if not self.valid_apikey():
            return self.apikey_error()

        format_param = request.args.get('format', 'json')

        if format_param.lower() == 'csv':
            return self.get_mobility_csv()
        elif format_param.lower() == 'json':
            return self.get_mobility_json()
        else:
            return self.badrequest_error()

    def get_mobility_json(self):
        data = list(self.mobility_collection.find())
        response = self.app.response_class(
            response=self.json.dumps(data, default=str, ensure_ascii=False),
            status=200,
            mimetype='application/json'
        )
        return response

    def get_mobility_csv(self):
        data = list(self.mobility_collection.find())

        if data:
            keys = data[0].keys()

            response = Response(content_type='text/csv')
            response.headers.set(
                'Content-Disposition', 'attachment', filename='international_mobility.csv')

            writer = csv.DictWriter(response.stream, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

            return response
        else:
            return self.app.response_class(
                response=self.json.dumps({'message': 'No data found.'}),
                status=404,
                mimetype='application/json'
            )
