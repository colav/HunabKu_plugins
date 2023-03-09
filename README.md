# HunabKu_plugins
Mono Repo for HunabKu Plugins 

# Table of Contents
* [Plugin explanation](#explanation)
* [Anatomy of a plugin package](#anatomy)
* [Endpoint documentation](#endpointdoc)
* [Handling the config](#config)
* [Creating a plugin release](#release)
* [Final remarks](#remarks)

## Plugin explanation <a name="explanation"></a>
This repository allows to host all plugins for Hunabku.
The directory `hunabku_template` is a template project to with a basic example, 
every project works as a python package and the hunabku plugin system works by name convention see ([python plugin docs](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-naming-convention))
in our case the name prefix is `hunabku_`

Instead copying the folder hunabku_template, you can run the next command to create you new plugin directory.
```sh
hunabku_server --generate_plugin myplugin
```
this is going to create a directory called  **HunabKu_myplugin*

## Anatomy of a plugin package <a name="anatomy"></a>
The packages have to have the next structure
```
HunabKu_myplugin
HunabKu_myplugin/README.md
HunabKu_myplugin/hunabku_myplugin
HunabKu_myplugin/hunabku_myplugin/__init__.py
HunabKu_myplugin/hunabku_myplugin/endpoints
HunabKu_myplugin/hunabku_myplugin/endpoints/Hello.py
HunabKu_myplugin/hunabku_myplugin/_version.py
HunabKu_myplugin/MANIFEST.in
HunabKu_myplugin/setup.py
```

All the class with the endpoints have to be located in the folder **HunabKu_myplugin/hunabku_myplugin/endpoints** 

The plugin have to be child class of **HunabkuPluginBase**,
in the template you will find a file Hello.py with a class Hello, 
as example.

The parameters for configuration have to be defined right before the class as class variable or class attribute  see ([python doc](https://docs.python.org/3/tutorial/classes.html#class-and-instance-variables))
```py
from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param

class Hello(HunabkuPluginBase):
    config = Config()
    config += Param(myvar="myvalue", doc="this is an example var")
    config.subcategory += Param(port=8080).doc("this is other way to set doc")
    config.subcategory += Param(host="localhost")  # may you want a var without doc
```


to define an endpoint you have to use the decorator endpoint where you can pass the url path and the methods such as GET, POST, PUT etc..

```py
    @endpoint('/hello', methods=['GET'])
    def hello(self):
        """
        @api {get} /hello/:id Hello
        @apiName Hello
        @apiGroup Template


        @apiSuccess {String} {'hello': 'world'}.
        """
        if self.valid_apikey():
            response = self.app.response_class(
                response=self.json.dumps({'hello': 'world'}),
                status=200,
                mimetype='application/json'
            )
            return response
        else:
            return self.apikey_error()
```

To handle the `response` you can access in the plugin to the flask app using `self.app` inside the plugin.

To handle the requests args you can call
`self.request.args`, `self.request.form` etc.. depending of the request methods you are using see ([flask request](https://flask.palletsprojects.com/en/2.2.x/quickstart/#accessing-request-data))

## Endpoint documentation <a name="endpointdoc"></a>
It's very important to write the documentation for your endpoint,
we are using apidocjs for that purpose see ([apidocjs](https://apidocjs.com/))
Hunabku will generate the docmentation for your plugin automatically, you can see it in the endpoint `/apidoc/index.htm`
If there is an error in you apidoc documentaion, the system will not allow you to start, until it is fixed.



## Handling the config <a name="config"></a>

Hunabku will automatically generate the config parameter in the config file when the plugin is installed.

To generate the config file including you config parameters run
```
hunabku_server --generate_config config.py
```

now a new section of configuration is genereted for your plugin, you can modified it as you prefer.

```py
# myvar
# this is an example var
config.hunabku_myplugin.Hello.Hello.myvar = "myvalue"

# port
# this is other way to set doc
config.hunabku_myplugin.Hello.Hello.subcategory.port = 8080

# host
# 
config.hunabku_myplugin.Hello.Hello.subcategory.host = "localhost"
```

# Creating a plugin release <a name="release"></a>
We have a github action that allow to create a release for any plugin in the mono repo.
* The first thing is to update the version of your plugin on 
`HunabKu_myplugin/hunabku_myplugin/_version.py`
* Lets go to [https://github.com/colav/HunabKu_plugins/releases](https://github.com/colav/HunabKu_plugins/releases) and click in **draf a new release**
* Click in choose new tag and write `Hunabku_mypluigin/v0.0.x` name of your plugin slash the version of your plugin
* Write the release notes
* click on publish relase

Now the github action will be activate and you can check the status of you package here[https://github.com/colav/HunabKu_plugins/actions/workflows/hunabku-plugin-publish.yml](https://github.com/colav/HunabKu_plugins/actions/workflows/hunabku-plugin-publish.yml)

# Final remarks <a name="remarks"></a>
* You can take a look in the already developed plugins to have examples or get inspiration for your plugins.
* Remember write elegant documentation for you endpoints.
* if you need help please open an issue.

Made with love ❤️ by Colav Team! 😃.
