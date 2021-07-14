#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yfinance as yf
from datetime import datetime
from datetime import timedelta
import sqlite3
from .utils import read_db_path
import pathlib
import logging
import numpy as np


class YFinance:

    # SQL requests
    TICKERS_REQUEST = 'SELECT * FROM tickers'
    TICKERS_INSERT = 'INSERT INTO tickers(name) VALUES (?)'
    TICKERS_DELETE = 'DELETE FROM tickers WHERE name=(?)'
    WRITE_DATA = 'REPLACE INTO ticker_values(value_date, ticker, ticker_value) VALUES (?,?,?)'
    READ_TICKERS = 'SELECT ticker from ticker_values GROUP by ticker'
    DELETE_ALL = 'DELETE from ticker_values'
    DELETE_ALL_TICKERS = 'DELETE from tickers'
    DELETE_SELECTION = 'DELETE * from ticker_values WHERE ticker=(?)'
    PARTIAL_CLEAN = 'DELETE from ticker_values WHERE value_date<(?)'
    TICKERS_WITH_MAX_DATES = 'SELECT ticker, MAX(value_date) FROM ticker_values GROUP BY ticker'

    # depth of stored data
    INITIAL_DEPTH = 5*360

    def __init__(self):

        # init logger
        logger_path = pathlib.Path(__file__).parent.resolve() / f'logs/log_db_{datetime.now().date()}_{datetime.now().time().hour}_{datetime.now().time().minute}.log'
        self.logger = logging.getLogger('db_logger')
        _log_format = f"[%(levelname)s] - %(asctime)s - %(message)s"
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(logger_path)
        fh.setFormatter(logging.Formatter(_log_format))
        self.logger.addHandler(fh)

        # opening db
        try:
            self.conn = sqlite3.connect(read_db_path())
        except Exception as ex:
            logging.error(ex)
            raise IOError

        # read tickers list from db
        self._tickers = ""
        self._tickers = self.read_tickers()

    def __del__(self):
        """
        Class destructor. Closes connection to the db.
        :return:
        """
        self.conn.close()

    ####
    # PROPERTIES DECORATORS
    ####

    @property
    def tickers(self):
        return self._tickers

    @tickers.getter
    def tickers(self):
        return self._tickers

    @tickers.setter
    def tickers(self, value):
        raise SyntaxError("The values can't set directly")

    ####
    # TICKERS OPERATIONS
    ####

    def add_tickers(self, tickers_list) -> bool:
        """
        Add tickers from the list to db
        :param tickers_list: Python list or a string, separated with a single space
        :return: database insertion result
        """
        return self.__proceed_tickers(tickers_list, True)

    def del_tickers(self, tickers_list):
        """
        Delete tickers from the list from DB
        :param tickers_list: Python list or a string, separated with a single space
        :return: database deletion result
        """
        return self.__proceed_tickers(tickers_list, False)

    def del_all_tickers(self) -> bool:
        """
        Delete all tickers from the DB
        :return:
        """
        try:
            cur = self.conn.cursor()
            cur.execute(self.DELETE_ALL_TICKERS)
            self.conn.commit()
            self._tickers = self.read_tickers()
            self.logger.info('All tickers are deleted from the DB')
            return True
        except Exception as ex:
            self.logger.error(f'An error while deleting all tickers. {ex}')

    def read_tickers(self) -> str:
        """
        Read tickers from the db
        :return: Tickers as a string. Tickers are separated with a single space.
        """
        current_row = self._tickers
        try:
            cur = self.conn.cursor()
            cur.execute(self.TICKERS_REQUEST)

            rows = cur.fetchall()
            self.logger.info(f'{len(rows)} tickers got from the DB')
            return ' '.join(sorted([row[1] for row in rows]))
        except Exception as ex:
            self.logger.error(f'Error occurs while reading tickers. {ex}')
            return current_row

    def __filter_tickers(self, tickers_list, flag) -> list:
        """
        Filter tickers list considering actual tickers list
        :param tickers_list: tickers list to filter
        :param flag: True - adding tickers, False - deleting tickers
        :return: filtered tickers list
        """
        # get current list of tickers and make a filtered list to insert
        current_tickers = self._tickers.split(' ')
        if flag:
            tickers_to_process = [ticker for ticker in tickers_list if ticker not in current_tickers]
        else:
            tickers_to_process = [ticker for ticker in tickers_list if ticker in current_tickers]

        return [(ticker,) for ticker in tickers_to_process]

    def __proceed_tickers(self, tickers_list, plus_minus) -> bool:
        """
        Add/delete ticker from the DB
        :param tickers_list: list of tickers to proceed
        :param plus_minus: True - add tickers, False - delete tickers
        :return: Operation success
        """
        # split a string into a Python list
        if isinstance(tickers_list, str):
            tickers_list = tickers_list.split(' ')

        # filter tickers to add comparing with current tickers list
        insert_data = self.__filter_tickers(tickers_list, plus_minus)

        if plus_minus:
            query = self.TICKERS_INSERT
            log_operation = 'inserted in DB'
        else:
            query = self.TICKERS_DELETE
            log_operation = 'deleted from DB'

        # db operation if true insertion list's length is greater than 0
        if len(insert_data) > 0:
            cur = self.conn.cursor()
            try:
                cur.executemany(query, insert_data)
                self.conn.commit()
                self.logger.info(f'{len(tickers_list)} tickers {log_operation}')
                return True
            except Exception as ex:
                self.logger.error(f'Tickers are not {log_operation}. Error message {ex}')
                return False
            finally:
                self._tickers = self.read_tickers()
        else:
            # no really data to insert
            return False

    ####
    # PROCEED DATA
    ####

    def initial_data_load(self, tickers=None) -> bool:
        """
        Loads data to the DB.
        Delete all data and load data for the defined initial period
        :param tickers: tickers for initial data load.
        If tickers is not None, only data of selected tickers will be cleaned and loaded
        :return: data load result
        """
        if tickers is None:
            load_for_tickers = self._tickers.split(' ')
        else:
            if isinstance(tickers, str):
                load_for_tickers = [tickers]
            elif isinstance(tickers, list):
                load_for_tickers = tickers
            else:
                raise ValueError(f'Improper parameter type: {type(tickers)}')
        finish_date = datetime.now().date()
        start_date = (datetime.now() - timedelta(days=self.INITIAL_DEPTH)).date()
        return self.__data_load(load_for_tickers, start_date, finish_date)

    def update_data(self, tickers=None) -> bool:
        """
        Updates data for selected (all tickers) since last saved date till today (now)
        :param tickers: tickers for data update, if None - tickers will be taken from the property
        :return: data load result
        """
        try:
            if not tickers:
                # update for all the tickers in the DB
                tickers = self.get_db_tickers()

            cur = self.conn.cursor()
            cur.execute(self.TICKERS_WITH_MAX_DATES)
            dates = [datetime.strptime(item[1], '%Y-%m-%d') for item in cur.fetchall() if item[0] in tickers]
            min_date = (min(dates) + timedelta(days=1)).date()
            self.logger.info(f'Data will be added/updated since {min_date}')

            self.__data_load(tickers, start_date=min_date, finish_date=datetime.now().date())
            self.logger.info('Data updated successfully')
            return True
        except Exception as ex:
            self.logger.error(f'Error while updating db. {ex}')
            return False

    def partial_data_clean(self, date=None) -> bool:
        """
        Delete data partially. If date is note None - all data before date.
        If not selected - all data before now() - initial period
        :param date: date to clean the data before
        :return: data clean result
        """
        if not date:
            date = (datetime.now() - timedelta(days=self.INITIAL_DEPTH)).date()
        try:
            cur = self.conn.cursor()
            cur.execute(self.PARTIAL_CLEAN, (date, ))
            self.conn.commit()
            self.logger.info(f'All the data before {date} is cleaned')
            return True
        except Exception as ex:
            self.logger.error(f'Error when partial cleaning. {ex}')
            return False

    def __data_load(self, tickers, start_date, finish_date) -> bool:
        """
        Private data load method. Used by public methods.
        :param tickers - list of tickers
        :param start_date - the beginning of load period
        :param finish_date - the end of load period
        :return:
        """

        # reading data
        try:
            self.logger.info(f'Reading data from Yahoo Finance')
            data = yf.download(tickers, start=start_date, end=finish_date)
            self.logger.info(f'YFinance data load call. Start date: {start_date}. Finish date: {finish_date}')
            data = data.loc[:, data.columns.get_level_values(0) == 'Close'].copy()
            if len(data.columns) == 1:
                data.rename(columns={'Close': tickers[0]}, inplace=True)
            else:
                data.columns = data.columns.droplevel()

            # database insertion
            self.logger.info(f'Inserting data into the DB')
            records = []
            for data_date in list(data.index):
                for ticker in tickers:
                    if not np.isnan(data.at[data_date, ticker]):
                        records.append((data_date.date(), ticker, data.at[data_date, ticker]))
        except Exception as ex:
            self.logger.error(f'Error while loading data using YFinance. {ex}')
            return False

        self.__save_data_to_db(records)
        return True

    def __save_data_to_db(self, data_list):
        """
        Saves fetched data to the DB
        :param data_list: Data to save (list of tuples)
        :return:
        """
        try:
            cur = self.conn.cursor()
            cur.executemany(self.WRITE_DATA, data_list)

            logging.info(f'{len(data_list)} rows inserted in db')

            self.conn.commit()
            return True

        except Exception as ex:
            self.logger.error(f'Error while writing to the DB. {ex}')
            return False

    def get_db_tickers(self) -> list:
        """
        Reads tickers from the DB
        :return: A list of tickers
        """
        try:
            cur = self.conn.cursor()
            cur.execute(self.READ_TICKERS)
            rows = cur.fetchall()
            self.logger.info('Tickers are read')
            return [row[0] for row in rows]
        except Exception as ex:
            self.logger.error(f'Error reading tickers from the DB. {ex}')
            return []

    def update_db(self) -> bool:
        # TODO revise
        """
        Updates information in the DB
        :return: boolean result
        """
        # first case - more tickers in _tickers than in db, less tickers in _tickers than in db
        try:
            tickers_list = self._tickers.split(' ')
            db_tickers = self.get_db_tickers()
            list_for_update = [item for item in tickers_list if item not in db_tickers]
            list_for_delete = [item for item in db_tickers if item not in tickers_list]

            if list_for_update:
                self.initial_data_load()

            if list_for_delete:
                cur = self.conn.cursor()
                deletion_data = [(item, ) for item in list_for_delete]
                cur.executemany(self.DELETE_SELECTION, deletion_data)

            self.conn.commit()
            self.logger.info(f'The DB updated successfully')
            return True
        except Exception as ex:
            self.logger.error(f'Error updating the database. {ex}')
            return False

    def del_all_from_db(self, tickers=None) -> bool:
        """
        Clears the database.
        :return: result (boolean)
        """
        try:
            cur = self.conn.cursor()
            cur.execute(self.DELETE_ALL)
            self.conn.commit()
            self.logger.info('The database is cleaned')
            return True
        except Exception as ex:
            self.logger.error(f'Error while cleaning the DB. {ex}')
            return False
