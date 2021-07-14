#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from app import YFinance
import sqlite3
from app.utils import read_db_path
import pandas as pd

unittest.TestLoader.sortTestMethodsUsing = None


class YahooDbTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.yahoo_object = YFinance()
        self.yahoo_object.del_all_tickers()
        self.yahoo_object.del_all_from_db()
        self.conn = sqlite3.connect(read_db_path())

    @unittest.skip('Passed')
    def test_add_delete_tickers(self):
        self.yahoo_object.add_tickers(['AAPL', 'TSLA'])
        self.assertEqual('AAPL TSLA', self.yahoo_object.tickers)
        self.yahoo_object.del_tickers('AAPL')
        self.assertEqual('TSLA', self.yahoo_object.tickers)

    def test_initial_load(self):
        self.yahoo_object.add_tickers(['AAPL', 'TSLA'])
        self.yahoo_object.initial_data_load()
        self.df = pd.read_sql_query("SELECT * from ticker_values", self.conn)
        self.assertEqual(2, len(self.df.ticker.unique()))

    def test_partial_initial_load(self):
        self.yahoo_object.add_tickers(['AAPL', 'TSLA'])
        self.yahoo_object.initial_data_load('AAPL')
        self.df = pd.read_sql_query("SELECT * from ticker_values", self.conn)
        self.assertEqual(1, len(self.df.ticker.unique()))

    def test_update_data(self):
        self.yahoo_object.add_tickers(['AAPL', 'TSLA'])
        self.yahoo_object.initial_data_load()
        self.yahoo_object.update_data()

    def tearDown(self) -> None:
        self.yahoo_object.del_all_tickers()
        self.yahoo_object.del_all_from_db()
        self.conn.close()
