from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from pymongo import MongoClient
from elasticsearch import Elasticsearch, __version__ as es_version
from elasticsearch_dsl import Search
import sys
import re
import time


class Scienti(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config += Param(es_uri="http://localhost:9200",
                    doc="Elastic Search url")
    config += Param(es_user="elastic",
                    doc="Elastic Search user")
    config += Param(es_pass="",
                    doc="Elastic Search password")

    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.db_uri)
        auth = (self.config.es_user, self.config.es_pass)
        if es_version[0] < 8:
            self.es = Elasticsearch(self.config.es_uri, http_auth=auth)
        else:
            self.es = Elasticsearch(self.config.es_uri, basic_auth=auth)

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

    def es_multi_match(self, keyword, fields, es_index):
        """
        Method to perform the elasticsearch multi_match query.
        """
        body = {
            "query": {
                "multi_match": {
                    "query": keyword,
                    "type": "phrase_prefix",
                    "fields": fields
                },
            }
        }
        s = Search(using=self.es, index=es_index)
        s = s.update_from_dict(body)
        s = s.extra(track_total_hits=True)
        s.execute()
        data = [hit.to_dict() for hit in s.scan()]
        return data

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
        @apiParam {String} institution Institution initials. supported example: udea, uec, unaula, univalle
        @apiParam {String} search Allows to search text keywords in several fields of the product collection using elastic search.
        @apiParam {String} group_id  Returns products for the given group id.


        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the products for the user
            curl -i http://apis.colav.co/scienti/product?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000000639
            # An specific product
            curl -i http://apis.colav.co/scienti/product?apikey=XXXX&model_year=2022&institution=udea&COD_RH=0000000639&COD_PRODUCTO=24
            # An specific product category
            curl -i http://apis.colav.co/scienti/product?apikey=XXXX&model_year=2022&institution=udea&SGL_CATEGORIA=ART-ART_A1
            # Text search for a keyword using elastic search
            curl -i http://apis.colav.co/scienti/product?apikey=XXXX&model_year=2022&institution=udea&search="machine learning"
            # return products for the given group id
            curl -i http://apis.colav.co/scienti/product?apikey=XXXX&model_year=2022&institution=udea&group_id=COL0008423


        """

        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_prod = self.request.args.get('COD_PRODUCTO')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')
            keyword = self.request.args.get('search')
            group_id = self.request.args.get('group_id')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            response = self.check_parameters(
                ['apikey', 'COD_RH', 'COD_PRODUCTO', 'SGL_CATEGORIA', 'model_year', 'institution', 'search', 'group_id'], self.request.args.keys())
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
                if keyword:
                    es_index = f'scienti_{institution}_{model_year}_product'

                    # not required extra fields for search, at least for product
                    # article, audiovisual, book, book_chapter, event, journal, journal_others, music_sheet
                    # oriented_thesis
                    fields = ["TXT_NME_PROD",
                              "TXT_RESUMEN_PROD",
                              "TXT_OBSERV_PROD",
                              "DSC_PROJETO",
                              # campos application_sector (es recursivo a 3 niveles)
                              # https://github.com/colav/KayPacha/blob/main/kaypacha/models/scienti/graph_schema_product.py#L636
                              "details.application_sector.TXT_NME_SECTOR_APLIC",
                              "details.application_sector.application_sector.TXT_NME_SECTOR_APLIC",
                              "details.application_sector.application_sector.application_sector.TXT_NME_SECTOR_APLIC",
                              # community
                              "details.community.TXT_CARACTERIZACION",
                              # course
                              "details.course.TXT_FINALIDAD",
                              # keywords
                              "details.keywords.TXT_NME_PALABRA_CLAVE",
                              # memory chapter
                              "details.memory_chapter.TXT_NME_PONENCIA",
                              "details.memory_chapter.TXT_NME_EVENTO",
                              # prod_art
                              "details.prod_art.prod_art_detail.TXT_NME_EVENTO",
                              "details.prod_art.prod_art_detail.knowledge_area.TXT_NME_AREA_FULL",
                              # technical
                              "details.technical.TXT_NME_COMERCIAL",
                              "details.technical.TXT_FINALIDAD"]

                    # get the start time
                    st = time.time()
                    data = self.es_multi_match(keyword, fields, es_index)
                    # get the end time
                    et = time.time()
                    # get the execution time
                    elapsed_time = et - st
                    print(f'Search for "{keyword}" in {es_index} Execution time:',
                          elapsed_time, 'seconds')
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if group_id:
                    data = list(self.db["product"].find(
                        {'group.COD_ID_GRUPO': group_id}, {"_id": 0}))
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
        @apiParam {String} institution institution initials. supported example: udea, uec, unaula, univalle
        @apiParam {String} search Allows to search text keywords in several fields of the network collection using elastic search.
        @apiParam {String} group_id  Returns networks for the given group id.

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
            # Text search for a keyword using elastic search
            curl -i http://apis.colav.co/scienti/network?apikey=XXXX&model_year=2022&institution=udea&search="educación"
            # return networks for the given group id
            curl -i http://apis.colav.co/scienti/network?apikey=XXXX&model_year=2022&institution=udea&group_id=COL0053803

        """

        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_red = self.request.args.get('COD_RED')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')
            keyword = self.request.args.get('search')
            group_id = self.request.args.get('group_id')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            response = self.check_parameters(
                ['apikey', 'COD_RH', 'COD_RED', 'SGL_CATEGORIA', 'model_year', 'institution', 'search', 'group_id'], self.request.args.keys())
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
                if keyword:
                    es_index = f'scienti_{institution}_{model_year}_network'

                    fields = ["TXT_NME_RED",
                              "details.community.TXT_NME_COMUNIDAD",
                              "details.community.TXT_CARACTERIZACION",
                              "details.community.product.TXT_NME_PROD",
                              "details.community.product.TXT_RESUMEN_PROD",
                              "details.community.product.TXT_OBSERV_PROD",
                              "details.community.product.DSC_PROJETO",
                              "details.community.project.TXT_NME_PROYECTO",
                              "details.community.project.TXT_OBSERV_PROYECTO",
                              "details.community.project.TXT_RESUMEN_PROYECTO",
                              "group.NME_GRUPO",
                              "group.TXT_PLAN_TRABAJO",
                              "group.TXT_ESTADO_ARTE",
                              "group.TXT_OBJETIVOS",
                              "group.TXT_PROD_DESTACADA",
                              "group.TXT_RETOS",
                              "group.TXT_VISION",
                              "group.knowledge_area.TXT_NME_AREA",  # recursive level 0
                              "group.knowledge_area.TXT_NME_AREA_FULL"
                              "group.knowledge_area.knowledge_area.TXT_NME_AREA",  # level 1
                              "group.knowledge_area.knowledge_area.TXT_NME_AREA_FULL"
                              "group.knowledge_area.knowledge_area.knowledge_area.TXT_NME_AREA",  # level 3
                              "group.knowledge_area.knowledge_area.knowledge_area.TXT_NME_AREA_FULL"
                              ]

                    # get the start time
                    st = time.time()
                    data = self.es_multi_match(keyword, fields, es_index)
                    # get the end time
                    et = time.time()
                    # get the execution time
                    elapsed_time = et - st
                    print(f'Search for "{keyword}" Execution time:',
                          elapsed_time, 'seconds')
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if group_id:
                    data = list(self.db["network"].find(
                        {'group.COD_ID_GRUPO': group_id}, {"_id": 0}))
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
                    sys.exc_info()), "execption": str(e)}
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
        @apiParam {String} institution institution initials. supported example: udea, uec, unaula, univalle
        @apiParam {String} search Allows to search text keywords in several fields of the project collection using elastic search.
        @apiParam {String} group_id  Returns projects for the given group id.

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
            # Text search for a keyword using elastic search
            curl -i http://apis.colav.co/scienti/project?apikey=XXXX&model_year=2022&institution=udea&search="machine learning"
            # return projects for the given group id
            curl -i http://apis.colav.co/scienti/project?apikey=XXXX&model_year=2022&institution=udea&group_id=COL0008423

        """

        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_projecto = self.request.args.get('COD_PROYECTO')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')
            keyword = self.request.args.get('search')
            group_id = self.request.args.get('group_id')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            response = self.check_parameters(
                ['apikey', 'COD_RH', 'COD_PROYECTO', 'SGL_CATEGORIA', 'model_year', 'institution', 'search', 'group_id'], self.request.args.keys())
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
                if keyword:
                    es_index = f'scienti_{institution}_{model_year}_project'

                    # only required fields for search are product and community, at least for project.
                    fields = ["TXT_NME_PROYECTO",
                              "TXT_OBSERV_PROYECTO",
                              "TXT_RESUMEN_PROYECTO",
                              "details.product.TXT_NME_PROD",
                              "details.product.TXT_RESUMEN_PROD",
                              "details.product.TXT_OBSERV_PROD",
                              "details.product.DSC_PROJETO",
                              "details.community.TXT_NME_COMUNIDAD",
                              "details.community.TXT_CARACTERIZACION"]
                    # get the start time
                    st = time.time()
                    data = self.es_multi_match(keyword, fields, es_index)
                    # get the end time
                    et = time.time()
                    # get the execution time
                    elapsed_time = et - st
                    print(f'Search for "{keyword}" Execution time:',
                          elapsed_time, 'seconds')
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response

                if group_id:
                    data = list(self.db["project"].find(
                        {'group.COD_ID_GRUPO': group_id}, {"_id": 0}))
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
        @apiParam {String} institution institution initials. supported example: udea, uec, unaula, univalle
        @apiParam {String} search Allows to search text keywords in several fields of the event collection using elastic search.
        @apiParam {String} group_id  Returns events for the given group id.

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
            # Text search for a keyword using elastic search
            curl -i http://apis.colav.co/scienti/event?apikey=XXXX&model_year=2022&institution=udea&search="machine learning"
            # return event for the given group id
            curl -i http://apis.colav.co/scienti/event?apikey=XXXX&model_year=2022&institution=udea&group_id=COL0008423

        """
        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_evento = self.request.args.get('COD_EVENTO')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')
            keyword = self.request.args.get('search')
            group_id = self.request.args.get('group_id')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            response = self.check_parameters(
                ['apikey', 'COD_RH', 'COD_PROYECTO', 'COD_EVENTO', 'model_year', 'institution', 'search', 'group_id'], self.request.args.keys())
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
                if keyword:
                    es_index = f'scienti_{institution}_{model_year}_event'

                    fields = ["TXT_NME_EVENTO",
                              "TXT_RESUMEN_EVENTO",
                              "TXT_ACTIVIDADES",
                              "project.TXT_NME_PROYECTO",
                              "project.TXT_RESUMEN_PROYECTO",
                              "details.product.TXT_NME_PROD",
                              "details.product.TXT_RESUMEN_PROD",
                              "details.product.TXT_OBSERV_PROD",
                              "details.product.DSC_PROJETO",
                              "details.keywords.TXT_NME_PALABRA_CLAVE",
                              "details.application_sector.TXT_NME_SECTOR_APLIC",  # recursive 2 times
                              "details.application_sector.application_sector.TXT_NME_SECTOR_APLIC",
                              ]
                    # get the start time
                    st = time.time()
                    data = self.es_multi_match(keyword, fields, es_index)
                    # get the end time
                    et = time.time()
                    # get the execution time
                    elapsed_time = et - st
                    print(f'Search for "{keyword}" Execution time:',
                          elapsed_time, 'seconds')
                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response
                if group_id:
                    data = list(self.db["event"].find(
                        {'group.COD_ID_GRUPO': group_id}, {"_id": 0}))
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

    @endpoint('/scienti/patent', methods=['GET'])
    def scienti_patent(self):
        """
        @api {get} /scienti/patent Scienti patent endpoint
        @apiName patent
        @apiGroup Scienti
        @apiDescription Allows to perform queries for patents,
                        model_year is mandatory parameter, if model year is the only
                        parameter passed, the endpoint returns all the dump of the database.

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} COD_RH  User primary key
        @apiParam {String} COD_PATENTE  patent key (require COD_RH)
        @apiParam {String} SGL_CATEGORIA  category of the network
        @apiParam {String} model_year  year of the scienti model, example: 2022
        @apiParam {String} institution institution initials. supported example: udea, uec, unaula, univalle
        @apiParam {String} search Allows to search text keywords in several fields of the patent collection using elastic search.

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
            # Text search for a keyword using elastic search
            curl -i http://apis.colav.co/scienti/patent?apikey=XXXX&model_year=2022&institution=udea&search="process"
        """
        if self.valid_apikey():
            cod_rh = self.request.args.get('COD_RH')
            cod_patente = self.request.args.get('COD_PATENTE')
            sgl_cat = self.request.args.get('SGL_CATEGORIA')
            model_year = self.request.args.get('model_year')
            institution = self.request.args.get('institution')
            keyword = self.request.args.get('search')

            response = self.check_required_parameters(self.request.args)
            if response is not None:
                return response
            response = self.check_parameters(
                ['apikey', 'COD_RH', 'COD_PROYECTO', 'COD_PATENTE', 'model_year', 'institution', 'search'], self.request.args.keys())
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
                if keyword:
                    es_index = f'scienti_{institution}_{model_year}_patent'

                    # only required field for search is technical, at least for patent.
                    fields = ["TXT_NME_PATENTE",
                              "details.technical.product.TXT_NME_PROD",
                              "details.technical.product.TXT_RESUMEN_PROD",
                              "details.technical.product.TXT_OBSERV_PROD",
                              "details.technical.product.DSC_PROJETO"
                              ]
                    # get the start time
                    st = time.time()
                    data = self.es_multi_match(keyword, fields, es_index)
                    # get the end time
                    et = time.time()
                    # get the execution time
                    elapsed_time = et - st
                    print(f'Search for "{keyword}" Execution time:',
                          elapsed_time, 'seconds')
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

    @endpoint('/scienti/info/', methods=['GET'])
    def scienti_info(self):
        """
        @api {get} /scienti/info Scienti info endpoint
        @apiName Info
        @apiGroup Scienti
        @apiDescription Allows to perform queries for information,
                        about avialable institution and ids.
        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} get Options are resume and ids, ids require additional parameters model_year and institution
        @apiParam {String} model_year  Year of the scienti model, example: 2022
        @apiParam {String} institution Institution initials. supported example: udea, uec, unaula, univalle

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # resume of scienti data bases and model_years
            curl -i http://apis.colav.co/scienti/info?apikey=XXXX&get=resume
            curl -i http://apis.colav.co/scienti/info?apikey=XXXX&get=ids&institution=udea&model_year=2022
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
                    dbs = self.dbclient.list_database_names()
                    for db in dbs:
                        if re.match(r'^scienti_*_*', db):
                            values = re.split("_", db)
                            cols = []
                            for col in self.dbclient[db].list_collection_names():
                                if "_checkpoint" not in col:
                                    count = self.dbclient[db][col].count_documents(
                                        {})
                                    cols.append({'name': col, 'count': count})
                            data.append(
                                {"institution": values[1], 'model_year': values[2], 'entities': cols})

                    response = self.app.response_class(
                        response=self.json.dumps(data),
                        status=200,
                        mimetype='application/json'
                    )
                    return response

                if option == "ids":
                    response = self.check_parameters(
                        ['apikey', 'get', 'institution', 'model_year'], self.request.args.keys())
                    if response is not None:
                        return response
                    model_year = self.request.args.get('model_year')
                    institution = self.request.args.get('institution')
                    db = f'scienti_{institution}_{model_year}'

                    response = self.check_db(db)
                    if response is not None:
                        return response

                    data = []
                    cols = []
                    for col in self.dbclient[db].list_collection_names():
                        if "_checkpoint" not in col:
                            ids = []
                            if col == "product":
                                ids = list(self.dbclient[db][col].find(
                                    {}, {'COD_RH': 1, 'COD_PRODUCTO': 1, '_id': 0}))
                            if col == "patent":
                                ids = list(self.dbclient[db][col].find(
                                    {}, {'COD_RH': 1, 'COD_PATENTE': 1, '_id': 0}))
                            if col == "event":
                                ids = list(self.dbclient[db][col].find(
                                    {}, {'COD_RH': 1, 'COD_EVENTO': 1, '_id': 0}))
                            if col == "network":
                                ids = list(self.dbclient[db][col].find(
                                    {}, {'COD_RH': 1, 'COD_RED': 1, '_id': 0}))
                            if col == "project":
                                ids = list(self.dbclient[db][col].find(
                                    {}, {'COD_RH': 1, 'COD_PROYECTO': 1, '_id': 0}))
                            if col == "institution_endorsement":
                                ids = list(self.dbclient[db][col].find(
                                    {}, {'COD_AVAL_INSTITUCION': 1, '_id': 0}))
                            cols.append({'name': col, 'ids': ids})
                    data.append({"institution": institution,
                                'model_year': model_year, 'entities': cols})

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
