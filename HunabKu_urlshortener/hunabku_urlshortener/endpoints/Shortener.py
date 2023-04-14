from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from flask import redirect
from pymongo import MongoClient, ReturnDocument, errors
import validators
import datetime
import base62

last_sec = None

class Shortener(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")

    config += Param(db_name="urlshortener",
                    doc="Mongo DB name")

    config += Param(collection_name="records",
                    doc="Mongo DB collection name to save the records")

    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.db_uri)
        self.db = self.dbclient[self.config.db_name]
        self.collection = self.db[self.config.collection_name]

    def validate_url(self, url):
        validation = validators.url(url)
        if validation:
            return True
        else:
            return False

    @endpoint('/s/<url_code>', methods=['GET', 'POST'])
    def url_id_end(self, url_code):
        """
        @api {get} /s/<url_code> Url code resolver
        @apiDescription redirects to an url given the url id
        @apiName resolver
        @apiGroup UrlShortener

        @apiSuccess  redirect to the website 
        """
        x = self.collection.find_one({"_id": url_code})
        if x:
            return redirect(x["url"])
        else:
            response = self.app.response_class(
                response=self.json.dumps({"error": "url_code not found"}),
                status=404,
                mimetype='application/json'
            )
            return response

    @endpoint('/shorturl_create', methods=['GET', 'POST'])
    def url_create_end(self):
        """
        @api {get} /shorturl_create Url create
        @apiDescription creates an url id
        @apiName create
        @apiGroup UrlShortener

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} url  url to encode

        @apiSuccess  redirect to the website 
        """
        if not self.valid_apikey():
            return self.apikey_error()
        url = ""
        args = {}
        if self.request.method == 'POST':
            args = self.request.form
        else:
            args = self.request.args
        if not self.valid_parameters(["url", "apikey"]):
            return self.badrequest_error()

        if not self.validate_url(args.get("url")):
            response = self.app.response_class(
                response=self.json.dumps(
                    {"error": "Bad request, invalid URL"}),
                status=400,
                mimetype='application/json'
            )
            return response
        
        
        def generate_code():
            """
            Generate a short code for a URL based on the current timestamp and a counter stored in MongoDB.

            Returns:
                A string with the short code for the URL.
            """
            global last_sec

            curr_secs = int(datetime.datetime.now().timestamp())

            counter_doc = self.collection.find_one_and_update(
                    {'_id': 'counter'},
                    {'$setOnInsert': {'last_timestamp': curr_secs}, '$inc': {'value': 1}},
                    upsert = True,
                    return_document = ReturnDocument.AFTER)

            last_sec = counter_doc['last_timestamp']
            counter = counter_doc['value']

            if curr_secs != last_sec:
                self.collection.update_one({'_id': 'counter'}, {'$set': {'last_timestamp': curr_secs, 'value': 0}})
                last_sec = curr_secs
                counter = 0
                
            #generate short code using base62 encoding
            short_code = base62.encode(int(f"{curr_secs}{counter}"))

            return short_code

        def insert_url(url):
            """
            Insert a URL and its corresponding short code in the MongoDB collection.

            Args:
                url: A string with the URL to be shortened.
            """
            short_code = generate_code()
            try:
                self.collection.insert_one({'_id': short_code, 'url': url})
            
            except errors.DuplicateKeyError:
                response = self.app.response_class(
                    response = self.json.dumps(
                    {"error": "Internal Server Error"}),
                status=500,
                mimetype='application/json')
                return response

            return short_code

        url = args["url"]
        short_code = insert_url(url)
        
        data = {"url_code": str(short_code)}
        response = self.app.response_class(
            response=self.json.dumps(data),
            status=200,
            mimetype='application/json'
        )
        return response