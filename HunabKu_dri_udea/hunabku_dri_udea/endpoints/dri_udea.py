from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from pymongo import MongoClient
from flask import Response, request
import csv
import re


class International(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")

    config += Param(db_name="international",
                    doc="Mongo DB name")

    config += Param(mobility_collection_name="mobilities",
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
        @apiParam {String="nacional","internacional"} [ambito] Agreement scope
        @apiParam {String="json","csv"}[format='json'] Response format

        @apiSuccess agreements Agreements records in JSON or CSV format.

        """

        if not self.valid_apikey():
            return self.apikey_error()

        format_param = request.args.get('format', 'json')
        ambito = request.args.get('ambito', request.args.get('ámbito'))

        if format_param.lower() == 'csv':
            return self.get_agreements_csv(ambito)
        elif format_param.lower() == 'json':
            return self.get_agreements_json(ambito)
        else:
            return self.badrequest_error()

    def get_ambito_query(self, ambito):
        if not ambito:
            return {}

        ambito_regex = re.compile(f'^{re.escape(ambito)}$', re.IGNORECASE)
        return {
            '$or': [
                {'ámbito': ambito_regex},
                {'ambito': ambito_regex},
                {'raw_payload.ÁMBITO': ambito_regex},
                {'raw_payload.ámbito': ambito_regex},
                {'raw_payload.ambito': ambito_regex},
            ]
        }

    def clean_raw_payload(self, record):
        raw_payload = record.get('raw_payload', {})
        excluded_columns = ['N', 'D:', 'Observaciones']
        renamed_columns = {
            'vía': 'VÍA',
            'via': 'VÍA',
            'ambito': 'ÁMBITO',
            'ámbito': 'ÁMBITO',
        }

        data = {}
        for key, value in raw_payload.items():
            if key in excluded_columns:
                continue
            data[renamed_columns.get(key, key)] = value
        return data

    def get_raw_payload_data(self, collection, ambito):
        records = list(collection.find(
            self.get_ambito_query(ambito),
            {'_id': 0, 'raw_payload': 1}
        ))
        return [self.clean_raw_payload(record) for record in records]

    def raw_payload_json_response(self, data):
        response = self.app.response_class(
            response=self.json.dumps(data, default=str, ensure_ascii=False),
            status=200,
            mimetype='application/json'
        )
        return response

    def raw_payload_csv_response(self, data, filename):
        if data:
            keys = data[0].keys()

            response = Response(content_type='text/csv')
            response.headers.set('Content-Disposition',
                                 'attachment', filename=filename)

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

    def get_agreements_json(self, ambito):
        data = self.get_raw_payload_data(self.agreements_collection, ambito)
        return self.raw_payload_json_response(data)

    def get_agreements_csv(self, ambito):
        data = self.get_raw_payload_data(self.agreements_collection, ambito)
        return self.raw_payload_csv_response(data, 'agreements.csv')

    @endpoint('/mobility', methods=['GET'])
    def get_mobility(self):
        """
        @api {get} /mobility Mobility records
        @apiName Mobility
        @apiGroup DRI UdeA

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String="nacional","internacional"} [ambito] Mobility scope
        @apiParam {String="json","csv"} format="json" Response format


        @apiSuccess {Object|file} mobility International mobility records in JSON or CSV format.

        """

        if not self.valid_apikey():
            return self.apikey_error()

        format_param = request.args.get('format', 'json')
        ambito = request.args.get('ambito', request.args.get('ámbito'))

        if format_param.lower() == 'csv':
            return self.get_mobility_csv(ambito)
        elif format_param.lower() == 'json':
            return self.get_mobility_json(ambito)
        else:
            return self.badrequest_error()

    def get_mobility_json(self, ambito):
        data = self.get_raw_payload_data(self.mobility_collection, ambito)
        return self.raw_payload_json_response(data)

    def get_mobility_csv(self, ambito):
        data = self.get_raw_payload_data(self.mobility_collection, ambito)
        return self.raw_payload_csv_response(data, 'international_mobility.csv')
