#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from app import YFinance

unittest.TestLoader.sortTestMethodsUsing = None


class YahooTestCase(unittest.TestCase):
    TICKERS_STRING = "AMD BAC"
    TICKERS_ADD_STRING = "GAZP.ME AAPL"
    TICKERS_ADD_LIST = ['TSLA', 'PLUG']

    def setUp(self) -> None:
        self.yahoo_object = YFinance()
        self.yahoo_object.add_tickers(self.TICKERS_STRING)

    def test_load_tickers(self):
        self.assertIsNotNone(self.yahoo_object.tickers)
        self.assertLess(0, len(self.yahoo_object.tickers))

    def test_add_tickers(self):
        self.assertTrue(self.yahoo_object.add_tickers(self.TICKERS_ADD_LIST))
        self.assertTrue(self.yahoo_object.add_tickers(self.TICKERS_ADD_STRING))

    def test_delete_tickers(self):
        self.assertTrue(self.yahoo_object.del_tickers(self.TICKERS_ADD_LIST))
        self.assertTrue(self.yahoo_object.del_tickers(self.TICKERS_ADD_STRING))

    def test_logger(self):
        self.assertIsNotNone(self.yahoo_object.tickers)