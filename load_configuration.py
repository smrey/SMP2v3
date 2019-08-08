import json

class LoadConfiguration:

    def __init__(self, path):
        self.path = path

    def load_config_file(self):
        '''
        :param pth: path to the location of the config file (usually location of the code)
        :return:
        '''
        with open(self.path + "bs.config.json") as config_file:
            try:
                config_json = json.load(config_file)
            except json.decoder.JSONDecodeError:
                raise Exception("Config file does not contain valid json")
        return config_json


    def get_authentication_token(self):
        '''
        :param pth: path to the location of the config file (usually location of the code)
        :return:
        '''
        with open(self.path + "bs.config.json") as config_file:
            try:
                config_json = json.load(config_file)
            except json.decoder.JSONDecodeError:
                raise Exception("Config file does not contain valid json")
        return 'Bearer ' + config_json.get("authenticationToken")

