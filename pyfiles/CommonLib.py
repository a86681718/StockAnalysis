import platform
from configparser import RawConfigParser


def getConf(section=None):
    conf = RawConfigParser()
    conf.read('../conf/default.properties')
    if section:
        return conf._sections[section]
    else:
        if platform.system() == 'Darwin':
            return conf._sections['MAC']
        elif platform.system() == 'Darwin':
            return conf._sections['GCP']
        else:
            return conf.defaults()