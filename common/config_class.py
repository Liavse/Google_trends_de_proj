import configparser

class Config:
    def __init__ (self, config_path='/usr/bin/config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    ''' for using regular config with config[x][y]'''
    def __getitem__(self, key):
        return self.config[key]

    ''' for using config with method get(x,y) '''
    def get (self,key,attribute):
        return self.config[key][attribute]
