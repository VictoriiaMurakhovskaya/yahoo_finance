import unittest
from app import YFinance
import configparser as cf
import pandas as pd


class YahooTestCase(unittest.TestCase):

    TICKERS_STRING = "GAZP.ME AAPL TSLA GE AMD BAC PLUG"

    def setUp(self) -> None:
        self.yahoo_object = YFinance()

    def test_load_tickers(self):
        self.assertEqual(self.TICKERS_STRING, self.yahoo_object.tickers)

    def test_load_initial_data(self):
        self.yahoo_object.initial_data_load()


