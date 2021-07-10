#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from app import YFinance

unittest.TestLoader.sortTestMethodsUsing = None


class YahooDbTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.yahoo_object = YFinance()
        self.yahoo_object.del_all_tickers()
        self.yahoo_object.del_all_from_db()

    @unittest.skip('Passed')
    def test_add_delete_tickers(self):
        self.assertEqual('AAPL TSLA', self.yahoo_object.tickers)

    @unittest.skip('Passed')
    def test_delete_tickers(self):
        self.yahoo_object.add_tickers('AAPL TSLA')
        self.yahoo_object.del_tickers(['AAPL'])
        self.assertEqual('TSLA', self.yahoo_object.tickers)

    @unittest.skip('Passed')
    def test_exchange_tickers(self):
        self.yahoo_object.add_tickers('AAPL TSLA')
        self.yahoo_object.del_tickers(['AAPL'])
        self.yahoo_object.add_tickers('GAZP.ME')
        self.assertEqual('GAZP.ME TSLA', self.yahoo_object.tickers)

    def test_get_data(self):
        self.yahoo_object.add_tickers('AAPL TSLA')
        self.yahoo_object.initial_data_load()
