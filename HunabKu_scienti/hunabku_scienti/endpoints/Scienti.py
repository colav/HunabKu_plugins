from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from pymongo import MongoClient
import pandas as pd
import os
import sys

class Scienti(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/", doc="MongoDB string connection")
    
    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.db_uri)

    def check_required_parameters(self, req_args):
        """
        Method to check mandatory parameters for the request.
        if a required parameter is not found, returns error code 400 (Bad Request)
        """
        model_year = req_args.get('model_year')
        institution = req_args.get('institution')
        if not model_year:
            # model year required
            data = {"error": "Bad Request",
                    "message": "model_year parameter is required, it was not provided."}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response

        if not institution:
            # institution required
            data = {"error": "Bad Request",
                    "message": "institution parameter is required, it was not provided. options are: udea, unaula, uec"}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response
        return None

    def check_db(self, db_name):
        """
        Method to check if the database exists, the database is a combination of scienti_{initials}_{year} ex: scienti_udea_2022
        """
        db_names = self.dbclient.list_database_names()
        if db_name not in db_names:
            data = {
                "error": "Bad Request", "message": f"invalid model_year or institution, db {db_name} not found."}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response
        return None

    @endpoint('/scienti/product', methods=['GET'])
    def scienti_product(self):
        """
        @api {get} /scienti/product Scienti prouduct endpoint
        @apiName product
        @apiGroup Scienti
        @apiDescription Allows to perform queries for products, 
                        model_year is mandatory parameter, if model year is the only 
                        parameter passed, the endpoint returns all the dump of the database. 

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} COD_RH  User primary key
        @apiParam {String} COD_PRODUCTO  Product key (require COD_RH)
        @apiParam {String} SGL_CATEGORIA  Category of the product
        @apiParam {String} model_year  Year of the scienti model, example: 2022
        @apiParam {String} institution Institution initials. supported example: udea, uec, unaula

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the products for the user
            curl -i http://apis.colav.co/scienti/product?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000000639
            # An specific product
            curl -i http://apis.colav.co/scienti/product?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000000639&COD_PRODUCTO=24
            # An specific product category
            curl -i http://apis.colav.co/scienti/product?apikey=XXXX&model_year=2022&institution=udea
        """

        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_prod = self.request.args.get('COD_PRODUCTO')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            db_name = f'scienti_{institution}_{model_year}'

            response = self.check_db(db_name)
            if response is not None:
                return response

            try:
                self.db = self.dbclient[db_name]
                data = []
                if cod_rh and cod_prod:
                    data = self.db["product"].find_one(
                        {'COD_RH': cod_rh, 'COD_PRODUCTO': cod_prod}, {"_id": 0})
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if cod_rh:
                    data = list(self.db["product"].find(
                        {'COD_RH': cod_rh}, {"_id": 0}))
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if sgl_cat:
                    data = list(self.db["product"].find(
                        {'SGL_CATEGORIA': sgl_cat}, {"_id": 0}))
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
            except:
                data = {"error": "Bad Request", "message": str(sys.exc_info())}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response
        else:
            return self.apikey_error()

    @endpoint('/scienti/network', methods=['GET'])
    def scienti_network(self):
        """
        @api {get} /scienti/network Scienti network endpoint
        @apiName network
        @apiGroup Scienti
        @apiDescription Allows to perform queries for networks, 
                        model_year is mandatory parameter, if model year is the only 
                        parameter passed, the endpoint returns all the dump of the database. 

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} COD_RH  User primary key
        @apiParam {String} COD_RED  network key (require COD_RH)
        @apiParam {String} SGL_CATEGORIA  category of the network
        @apiParam {String} model_year  year of the scienti model, example: 2022
        @apiParam {String} institution institution initials. supported example: udea, uec, unaula

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the networks for the user
            curl -i http://apis.colav.co/scienti/network?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000172057
            # An specific network
            curl -i http://apis.colav.co/scienti/network?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000172057&COD_RED=1
            # An specific network category
            curl -i http://apis.colav.co/scienti/network?apikey=XXXX&model_year=2022&institution=udea&SGL_CATEGORIA=RC-RC_A
        """

        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_red = self.request.args.get('COD_RED')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            db_name = f'scienti_{institution}_{model_year}'

            response = self.check_db(db_name)
            if response is not None:
                return response

            try:
                self.db = self.dbclient[db_name]
                data = []
                if cod_rh and cod_red:
                    cod_red = int(cod_red)
                    data = self.db["network"].find_one(
                        {'COD_RH': cod_rh, 'COD_RED': cod_red}, {"_id": 0})
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if cod_rh:
                    data = list(self.db["network"].find(
                        {'COD_RH': cod_rh}, {"_id": 0}))
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if sgl_cat:
                    data = list(self.db["network"].find(
                        {'SGL_CATEGORIA': sgl_cat}, {"_id": 0}))
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

            except:
                data = {"error": "Bad Request", "message": str(sys.exc_info())}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response
        else:
            return self.apikey_error()

    @endpoint('/scienti/project', methods=['GET'])
    def scienti_project(self):
        """
        @api {get} /scienti/project Scienti project endpoint
        @apiName project
        @apiGroup Scienti
        @apiDescription Allows to perform queries for projects, 
                        model_year is mandatory parameter, if model year is the only 
                        parameter passed, the endpoint returns all the dump of the database. 

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} COD_RH  User primary key
        @apiParam {String} COD_PROYECTO  project key (require COD_RH)
        @apiParam {String} SGL_CATEGORIA  category of the network
        @apiParam {String} model_year  year of the scienti model, example: 2022
        @apiParam {String} institution institution initials. supported example: udea, uec, unaula

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the projects for the user
            curl -i http://apis.colav.co/scienti/project?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000000930
            # An specific project
            curl -i http://apis.colav.co/scienti/project?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000000930&COD_PROYECTO=1
            # An specific project category
            curl -i http://apis.colav.co/scienti/project?apikey=XXXX&model_year=2022&institution=udea&SGL_CATEGORIA=PID-00
        """

        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_projecto = self.request.args.get('COD_PROYECTO')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            db_name = f'scienti_{institution}_{model_year}'

            response = self.check_db(db_name)
            if response is not None:
                return response

            try:
                self.db = self.dbclient[db_name]
                data = []
                if cod_rh and cod_projecto:
                    data = self.db["project"].find_one(
                        {'COD_RH': cod_rh, 'COD_PROYECTO': cod_projecto}, {"_id": 0})
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if cod_rh:
                    data = list(self.db["project"].find(
                        {'COD_RH': cod_rh}, {"_id": 0}))
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if sgl_cat:
                    data = list(self.db["project"].find(
                        {'SGL_CATEGORIA': sgl_cat}, {"_id": 0}))
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

            except:
                data = {"error": "Bad Request", "message": str(sys.exc_info())}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response
        else:
            return self.apikey_error()

    @endpoint('/scienti/event', methods=['GET'])
    def scienti_event(self):
        """
        @api {get} /scienti/event Scienti event endpoint
        @apiName event
        @apiGroup Scienti
        @apiDescription Allows to perform queries for events, 
                        model_year is mandatory parameter, if model year is the only 
                        parameter passed, the endpoint returns all the dump of the database. 

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} COD_RH  User primary key
        @apiParam {String} COD_EVENTO  event key (require COD_RH)
        @apiParam {String} SGL_CATEGORIA  category of the network
        @apiParam {String} model_year  year of the scienti model, example: 2022
        @apiParam {String} institution institution initials. supported example: udea, uec, unaula

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the events for the user
            curl -i http://apis.colav.co/scienti/event?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000000016
            # An specific event
            curl -i http://apis.colav.co/scienti/event?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000000016&COD_EVENTO=2
            # An specific event category
            curl -i http://apis.colav.co/scienti/event?apikey=XXXX&model_year=2022&institution=udea&SGL_CATEGORIA=EC-EC_B
        """
        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_evento = self.request.args.get('COD_EVENTO')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            db_name = f'scienti_{institution}_{model_year}'

            response = self.check_db(db_name)
            if response is not None:
                return response

            try:
                self.db = self.dbclient[db_name]
                data = []
                if cod_rh and cod_evento:
                    data = self.db["event"].find_one(
                        {'COD_RH': cod_rh, 'COD_EVENTO': int(cod_evento)}, {"_id": 0})
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if cod_rh:
                    data = list(self.db["event"].find(
                        {'COD_RH': cod_rh}, {"_id": 0}))
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if sgl_cat:
                    data = list(self.db["event"].find(
                        {'SGL_CATEGORIA': sgl_cat}, {"_id": 0}))
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

            except:
                data = {"error": "Bad Request", "message": str(sys.exc_info())}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response
        else:
            return self.apikey_error()

    @endpoint('/scienti/patent', methods=['GET'])
    def patent_event(self):
        """
        @api {get} /scienti/patent Scienti patent endpoint
        @apiName event
        @apiGroup Scienti
        @apiDescription Allows to perform queries for patents, 
                        model_year is mandatory parameter, if model year is the only 
                        parameter passed, the endpoint returns all the dump of the database. 

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} COD_RH  User primary key
        @apiParam {String} COD_PATENTE  event key (require COD_RH)
        @apiParam {String} SGL_CATEGORIA  category of the network
        @apiParam {String} model_year  year of the scienti model, example: 2022
        @apiParam {String} institution institution initials. supported example: udea, uec, unaula

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the patents for the user
            curl -i http://apis.colav.co/scienti/patent?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000204234
            # An specific patent
            curl -i http://apis.colav.co/scienti/patent?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000204234&COD_PATENTE=2
            # An specific patent category
            curl -i http://apis.colav.co/scienti/patent?apikey=XXXX&model_year=2022&institution=udea&SGL_CATEGORIA=PIV-00
        """
        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_patente = self.request.args.get('COD_PATENTE')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            db_name = f'scienti_{institution}_{model_year}'

            response = self.check_db(db_name)
            if response is not None:
                return response

            try:
                self.db = self.dbclient[db_name]
                data = []
                if cod_rh and cod_patente:
                    data = self.db["patent"].find_one(
                        {'COD_RH': cod_rh, 'COD_PATENTE': int(cod_patente)}, {"_id": 0})
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if cod_rh:
                    data = list(self.db["patent"].find(
                        {'COD_RH': cod_rh}, {"_id": 0}))
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if sgl_cat:
                    data = list(self.db["patent"].find(
                        {'SGL_CATEGORIA': sgl_cat}, {"_id": 0}))
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

            except:
                data = {"error": "Bad Request", "message": str(sys.exc_info())}
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=400,
                    mimetype='application/json'
                )
                return response
        else:
            return self.apikey_error()
