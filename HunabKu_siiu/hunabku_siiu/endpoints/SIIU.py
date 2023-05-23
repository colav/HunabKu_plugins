from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from pymongo import MongoClient
from elasticsearch import Elasticsearch, __version__ as es_version
from elasticsearch_dsl import Search
import time


class SIIU(HunabkuPluginBase):
    config = Config()
    config += Param(mdb_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config += Param(mdb_name="siiu",
                    doc="MongoDB name for SIIU")
    config += Param(es_uri="http://localhost:9200",
                    doc="Elastic Search url")
    config += Param(es_user="elastic",
                    doc="Elastic Search user")
    config += Param(es_pass="colav",
                    doc="Elastic Search password")
    config += Param(es_project_index="siiu_project",
                    doc="Elastic Search siiu project index name")

    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.mdb_uri)
        auth = (self.config.es_user, self.config.es_pass)
        if es_version[0] < 8:
            self.es = Elasticsearch(self.config.es_uri, http_auth=auth)
        else:
            self.es = Elasticsearch(self.config.es_uri, basic_auth=auth)

    def check_index(self):
        if not self.es.indices.exists(index=self.config.es_project_index):
            response = self.app.response_class(
                response=self.json.dumps(
                    {"msg": f"Internal error, index {self.config.es_project_index} not found in Elastic Search"}),
                status=500,
                mimetype='application/json'
            )
            return response
        return None

    @endpoint('/siiu/project', methods=['GET'])
    def siiu_project(self):
        """
        @api {get} /siiu/project Project
        @apiName Project
        @apiGroup SIIU
        @apiDescription Allows to perform queries for projects,
                        you can search by project id or by keyword.
                        The search by keyword perform a search in teh fields of text
                        NOMBRE_CORTO, NOMBRE_COMPLETO, PALABRAS_CLAVES, descriptive_text.TEXTO_INGRESADO
                        lots of text where indexed for this search.

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} search  keyword for text search.
        @apiParam {String} CODIGO  project id.
        @apiParam {String} group_code  Colciencias Group ID ex:"COL0008423"
        @apiParam {String} group_name  name of the research group (returns the projects for this group)
        @apiParam {String} participant_name  name of the project participant (returns the projects for this participant)
        @apiParam {String} participant_id  id of the participant (returns the projects for this participant)

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the products for the user
            curl -i http://apis.colav.co/siiu/project?apikey=XXXX&search=keyword
            # An specific product
            curl -i http://apis.colav.co/siiu/project?apikey=XXXX&CODIGO=2013-86
            # An projects given a group id
            curl -i http://apis.colav.co/siiu/project?apikey=XXXX&group_code=COL0008423
            # An projects given a group name
            curl -i http://apis.colav.co/siiu/project?apikey=XXXX&group_name="psicologia cognitiva"
            # An projects given a participant name
            curl -i http://apis.colav.co/siiu/project?apikey=XXXX&participant_name="Diego Alejandro Restrepo Quintero"
            # An projects given a participant id
            curl -i http://apis.colav.co/siiu/project?apikey=XXXX&participant_id="xxxx"

        """
        if self.valid_apikey():

            keyword = self.request.args.get('search')
            codigo = self.request.args.get('CODIGO')
            grp_codigo = self.request.args.get('group_code')
            group_name = self.request.args.get('group_name')
            participant_name = self.request.args.get('participant_name')
            participant_id = self.request.args.get('participant_id')

            if keyword:
                check = self.check_index()
                if check is not None:
                    return check

                body = {"query": {
                    "bool": {
                        "should": [
                            {"match": {"NOMBRE_CORTO": keyword}},
                            {"match": {"NOMBRE_COMPLETO": keyword}},
                            {"match": {"PALABRAS_CLAVES": keyword}},
                            {"match": {"descriptive_text.TEXTO_INGRESADO": keyword}}
                        ]
                    }
                }
                }
                # get the start time
                st = time.time()
                s = Search(using=self.es, index=self.config.es_project_index)
                s = s.update_from_dict(body)
                s = s.extra(track_total_hits=True)
                s.execute()
                data = [hit.to_dict() for hit in s.scan()]
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=200,
                    mimetype='application/json'
                )
                # get the end time
                et = time.time()
                # get the execution time
                elapsed_time = et - st
                print(f'Search for "{keyword}" Execution time:',
                      elapsed_time, 'seconds')
                return response
            if codigo:
                data = list(self.dbclient[self.config.mdb_name]
                            ["project"].find({'CODIGO': codigo}, {'_id': 0, }))
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=200,
                    mimetype='application/json'
                )
                return response
            if grp_codigo:
                data = list(self.dbclient[self.config.mdb_name]
                            ["project"].find({"project_participant.group.CODIGO_COLCIENCIAS": grp_codigo}, {"_id": 0}))
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=200,
                    mimetype='application/json'
                )
                return response
            if participant_id:
                data = list(self.dbclient[self.config.mdb_name]
                            ["project"].find({"project_participant.PERSONA_NATURAL": participant_id}, {"_id": 0}))
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=200,
                    mimetype='application/json'
                )
                return response

            if group_name:
                check = self.check_index()
                if check is not None:
                    return check
                body = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match_phrase": {
                                    "project_participant.group.NOMBRE_COMPLETO": group_name}},
                            ]
                        }
                    }
                }

                # get the start time
                st = time.time()
                s = Search(using=self.es, index=self.config.es_project_index)
                s = s.update_from_dict(body)
                s = s.extra(track_total_hits=True)
                s.execute()
                data = [hit.to_dict() for hit in s.scan()]
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=200,
                    mimetype='application/json'
                )
                # get the end time
                et = time.time()
                # get the execution time
                elapsed_time = et - st
                print(f'Search for "{group_name}" Execution time:',
                      elapsed_time, 'seconds')
                return response

            if participant_name:
                check = self.check_index()
                if check is not None:
                    return check
                body = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match_phrase": {
                                    "project_participant.NOMBRE_COMPLETO": participant_name}},
                            ]
                        }
                    }
                }

                # get the start time
                st = time.time()
                s = Search(using=self.es, index=self.config.es_project_index)
                s = s.update_from_dict(body)
                s = s.extra(track_total_hits=True)
                s.execute()
                data = [hit.to_dict() for hit in s.scan()]
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=200,
                    mimetype='application/json'
                )
                # get the end time
                et = time.time()
                # get the execution time
                elapsed_time = et - st
                print(f'Search for "{group_name}" Execution time:',
                      elapsed_time, 'seconds')
                return response

            data = {
                "error": "Bad Request", "message": "invalid parameters, please select the right combination of parameters."}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response

        else:
            return self.apikey_error()

    @endpoint('/siiu/info', methods=['GET'])
    def config_end(self):
        """
        @api {get} /siiu/info Info
        @apiName Info
        @apiGroup SIIU
        @apiDescription Allows to get information of the projects such as ids (CODIGO)

        @apiParam {String} apikey  Credential for authentication

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the products for the user
            curl -i http://apis.colav.co/siiu/info?apikey=XXXX
        """
        data = list(self.dbclient[self.config.mdb_name]
                    ["project"].find({}, {'_id': 0, 'CODIGO': 1}))
        response = self.app.response_class(
            response=self.json.dumps(data),
            status=200,
            mimetype='application/json'
        )
        return response
