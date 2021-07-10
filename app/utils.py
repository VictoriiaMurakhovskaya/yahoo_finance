import configparser as cp
import pathlib


def read_db_path():
    cf = cp.ConfigParser()
    cf_path = pathlib.Path(__file__).parent.resolve() / 'app.cfg'
    cf.read(cf_path)
    return pathlib.Path(__file__).parent.resolve() / cf.get('FILES', 'db')