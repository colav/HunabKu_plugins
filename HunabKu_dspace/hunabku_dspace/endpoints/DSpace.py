from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from pymongo import MongoClient
import sys
import re


class DSpace(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config += Param(mdb_name="oxomoc",
                    doc="MongoDB name for DSpace")

    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.db_uri)
        self.db = self.dbclient[self.config.mdb_name]

    def check_required_parameters(self, req_args):
        """
        Method to check mandatory parameters for the request.
        if a required parameter is not found, returns error code 400 (Bad Request)
        """
        institution = req_args.get('institution')

        if not institution:
            # institution required
            data = {"error": "Bad Request",
                    "message": "institution parameter is required, it was not provided. options are: udea, unaula, uec, univalle"}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response
        return None

    def check_parameters(self, end_params, req_args):
        """
        Method to check is the parameters passed to the endpoint are valid,
        if unkown parameter is passed and Bad request is returned.
        """
        for rarg in req_args:
            if rarg not in end_params:
                data = {"error": "Bad Request",
                        "message": f"invalid parameter {rarg} passed. please fix your request. Valid parameters are {end_params}"}
                response = self.app.response_class(response=self.json.dumps(data),
                                                   status=400,
                                                   mimetype='application/json'
                                                   )
                return response
        return None

    def check_collection(self, col_name):
        """
        Method to check if the collection exists, the colletion is a combination of dspace_{initials}_records ex: dspace_udea_records
        """
        col_names = self.db.list_collection_names()
        if col_name not in col_names:
            data = {
                "error": "Bad Request", "message": f"invalid institution, collection {col_name} not found in database {self.config.mdb_name}. Please check info endpoint for available institutions."}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response
        return None

    @endpoint('/dspace/product', methods=['GET'])
    def dspace_product(self):
        """
        @api {get} /dspace/product DSpace prouduct endpoint
        @apiName product
        @apiGroup DSpace
        @apiDescription Allows to perform queries for products on DSpace,
                        institution is mandatory parameter.

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} id  DSpace id of the product
        @apiParam {String} institution Institution initials. supported example: udea, uec, unaula, univalle

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the products for the institution
            curl -i http://apis.colav.co/dspace/product?apikey=XXXX&institution=udea
            # An specific product
            curl -i http://apis.colav.co/dspace/product?apikey=XXXX&institution=udea&id=oai:bibliotecadigital.udea.edu.co:10495/1489
        """

        if self.valid_apikey():
            pid = self.request.args.get('id')
            institution = self.request.args.get('institution')
            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response

            col_name = f'dspace_{institution}_records'

            response = self.check_collection(col_name)
            if response is not None:
                return response

            try:
                data = []
                if pid:
                    data = list(self.db[col_name].find(
                        {'_id': pid}))
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if institution:
                    data = list(self.db[col_name].find())
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response

                data = {
                    "error": "Bad Request", "message": "invalid parameters, please select the right combination of parameters."}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response
            except Exception as e:
                data = {"error": "Bad Request", "message": str(
                    sys.exc_info()), "execption": str(e)}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response
        else:
            return self.apikey_error()

    @endpoint('/dspace/info/', methods=['GET'])
    def dspace_info(self):
        """
        @api {get} /dspace/info DSpace info endpoint
        @apiName Info
        @apiGroup DSpace
        @apiDescription Allows to perform queries for information,
                        about avialable institution and ids.
        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} get Options are resume and ids, ids require additional parameter institution
        @apiParam {String} institution Institution initials. supported example: udea, uec, unaula, univalle

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # resume of dspace database
            curl -i http://apis.colav.co/dspace/info?apikey=XXXX&get=resume
            curl -i http://apis.colav.co/dspace/info?apikey=XXXX&get=ids&institution=udea
        """
        if self.valid_apikey():
            option = self.request.args.get('get')
            if not option:
                response = self.check_parameters(
                    ['apikey', 'get'], self.request.args.keys())
                if response is not None:
                    return response
            try:
                if option == "resume":
                    data = []
                    cols = self.db.list_collection_names()
                    for col in cols:
                        if re.match(r'^dspace_.*._records$', col):
                            values = re.split("_", col)
                            info = []
                            count = self.db[col].count_documents({})
                            info.append({'name': 'records', 'count': count})
                            data.append(
                                {"institution": values[1], 'info': info})

                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response

                if option == "ids":
                    response = self.check_parameters(
                        ['apikey', 'get', 'institution'], self.request.args.keys())
                    if response is not None:
                        return response
                    institution = self.request.args.get('institution')
                    col = f'dspace_{institution}_records'

                    response = self.check_collection(col)
                    if response is not None:
                        return response

                    data = []
                    cols = []
                    ids = list(self.db[col].find({}, {"_id": 1}))
                    cols.append({'name': 'records', 'ids': ids})
                    data.append({"institution": institution, 'info': cols})

                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response

                data = {
                    "error": "Bad Request", "message": "invalid parameters, please select the right combination of parameters"}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response

            except Exception as e:
                data = {"error": "Bad Request", "message": str(
                    sys.exc_info()), "exception": str(e)}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response
        else:
            return self.apikey_error()
