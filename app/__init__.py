import yfinance as yf
from datetime import datetime
from datetime import timedelta
import sqlite3
import configparser as cp
import pathlib
import logging
import pandas as pd
import numpy as np


class YFinance:
    TICKERS_REQUEST = 'SELECT * FROM tickers'
    WRITE_DATA = 'INSERT INTO ticker_values(value_date, ticker, ticker_value) VALUES (?,?,?)'
    INITIAL_DEPTH = 5 * 365

    def __init__(self):

        # init logger
        logger_path = pathlib.Path(
            __file__).parent.resolve() / f'logs/log_db_{datetime.now().date()}_{datetime.now().time().hour}_{datetime.now().time().minute}.log'
        self.logger = logging.getLogger('db_logger')
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(logger_path)
        self.logger.addHandler(fh)

        # read config
        cf = cp.ConfigParser()
        self.cf_path = pathlib.Path(__file__).parent.resolve() / 'app.cfg'
        cf.read(self.cf_path)
        self.db_file = pathlib.Path(__file__).parent.resolve() / cf.get('FILES', 'db')

        # opening db
        try:
            self.conn = sqlite3.connect(self.db_file)
        except Exception as ex:
            logging.error(ex)
            raise IOError

        # read tickers list from db
        self._tickers = self.read_tickers_from_db()

    def __del__(self):
        self.conn.close()

    @property
    def tickers(self):
        return self._tickers

    @tickers.getter
    def tickers(self):
        return self._tickers

    @tickers.setter
    def tickers(self, value):
        raise SyntaxError("The values can't set directly")

    def read_tickers_from_db(self):

        cur = self.conn.cursor()
        cur.execute(self.TICKERS_REQUEST)

        rows = cur.fetchall()
        return ' '.join([row[1] for row in rows])

    def initial_data_load(self):

        # reading data
        finish_date = datetime.now().date()
        start_date = (datetime.now() - timedelta(days=self.INITIAL_DEPTH)).date()
        data = yf.download(self._tickers, start=start_date, end=finish_date)
        data = data.loc[:, data.columns.get_level_values(0) == 'Close'].copy()
        data.columns = data.columns.droplevel()

        data.to_pickle('test_data.pickle')

        # database insertion
        records = []
        for data_date in list(data.index):
            for ticker in self._tickers.split(' '):
                if not np.isnan(data.at[data_date, ticker]):
                    records.append((data_date.date(), ticker, data.at[data_date, ticker]))

        self.save_data(records)

    def save_data(self, data_list):
        cur = self.conn.cursor()
        cur.executemany(self.WRITE_DATA, data_list)

        logging.info(f'{len(data_list)} rows inserted in db')

        self.conn.commit()

    def update_db(self):
        pass

    def update_tickers_list(self):
        pass
