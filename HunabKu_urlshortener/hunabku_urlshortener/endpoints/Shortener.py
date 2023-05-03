from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from flask import redirect
from pymongo import MongoClient, errors
import validators
import datetime
import base62


class Shortener(HunabkuPluginBase):

    counter = 0
    last_sec = None

    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")

    config += Param(db_name="urlshortener",
                    doc="Mongo DB name")

    config += Param(collection_name="records",
                    doc="Mongo DB collection name to save the records")
    
    config += Param(maxtries=3,
                    doc="Number of times to try generating a new short code if the insertion fails")

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


    def generate_code(self):
        """
        Generate a short code for a URL based on the current timestamp.

        Returns:
            A string with the short code for the URL.
        """

        curr_secs = int(datetime.datetime.now().timestamp())

        self.counter += 1

        if curr_secs != self.last_sec:
            self.last_sec = curr_secs
            self.counter = 0
            
        #generate short code using base62 encoding
        short_code = base62.encode(int(f"{curr_secs}{self.counter}"))

        return short_code    
    
   
    def insert_url(self, url):
        """
        Insert a URL and its corresponding short code in the MongoDB collection.

        Args:
            url: A string with the URL to be shortened.
        """
        tries = 0
        
        while tries < self.config.maxtries:
            short_code = self.generate_code()
            try:
                self.collection.insert_one({'_id': short_code, 'url': url})
                return short_code
            
            except errors.DuplicateKeyError:
                tries += 1

        response = self.app.response_class(
            response = self.json.dumps(
                {"error": "Could not generate a unique short code"}),
            status=500,
            mimetype='application/json'
        )
        return response


    @endpoint('/<url_code>', methods=['GET', 'POST'])
    def url_id_end(self, url_code):
        """
        @api {get} /<url_code> URL code resolver
        @apiDescription Redirects to an URL given a shortened code
        @apiName resolver
        @apiGroup URLShortener

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


    @endpoint('/create', methods=['GET', 'POST'])
    def url_create_end(self):
        """
        @api {get} /create  Create a shortened URL
        @apiDescription Creates a unique shortened code
        @apiName create
        @apiGroup URLShortener

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} url  URL to encode

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
    

        url = args["url"]
        short_code = self.insert_url(url)        
        data = {"url_code": str(short_code)}
        response = self.app.response_class(
            response=self.json.dumps(data),
            status=200,
            mimetype='application/json'
        )
        return response