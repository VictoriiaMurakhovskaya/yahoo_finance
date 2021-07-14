#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import numpy as np
from .utils import read_db_path
import pathlib
from datetime import datetime
from random import randrange


class Portfolio:
    ITERATION_LIMIT = 100
    GENERATED_PORTFOLIOS = 100

    def __init__(self):
        """
        Class constructor
        """
        self._df = None
        self._prof_df = None

        # opening db
        try:
            self.conn = sqlite3.connect(read_db_path())
        except Exception as ex:
            print(ex)
            raise IOError
        pass

    def __del__(self):
        """
        Class destructor. Closes connection to the db.
        :return:
        """
        self.conn.close()

    def load_data(self):
        """
        Loads data from the application DB
        :return:
        """
        try:
            self._df = pd.read_sql_query("SELECT * from ticker_values", self.conn)
            self._df.drop(columns=['id'], inplace=True)
            self._df = self._df.pivot_table(index=['value_date'], columns=['ticker'], values=['ticker_value']).copy()
            self._df.columns = self._df.columns.droplevel()
            return True
        except Exception as ex:
            print(ex)
            return False

    def load_test_data(self):
        """
        Loads data from included binary file sample.pickle.
        Data is used for test
        :return:
        """
        try:
            cf_path = pathlib.Path(__file__).parent.resolve() / 'test_data.pickle'
            self._df = pd.read_pickle(cf_path)
            return True
        except Exception as ex:
            print(ex)
            return False

    def make_test_data(self):
        """
        Make test data locally
        Used for testing purpose
        :return: nothing
        """
        visa = [93.78, 99.56, 103.52, 105.24, 109.98, 112.59, 114.02, 124.23, 122.94, 119.62, 126.88, 130.72]
        macy = [23.24, 23.75, 20.77, 21.82, 18.76, 23.8, 25.19, 25.95, 29.41, 29.74, 31.07, 34.91]
        apple = [144.02, 148.73, 164, 154.12, 169.04, 171.85, 169.23, 167.43, 178.12, 167.78, 165.26, 186.87]
        att = [37.73, 39, 37.46, 39.17, 33.65, 36.38, 38.88, 37.45, 36.3, 35.65, 32.7, 32.32]
        data_index = [datetime(2017, 6, 30),
                      datetime(2017, 7, 31),
                      datetime(2017, 8, 31),
                      datetime(2017, 9, 30),
                      datetime(2017, 10, 31),
                      datetime(2017, 11, 30),
                      datetime(2017, 12, 31),
                      datetime(2018, 1, 31),
                      datetime(2018, 2, 28),
                      datetime(2018, 3, 31),
                      datetime(2018, 4, 30),
                      datetime(2018, 5, 31)
                      ]
        self._df = pd.DataFrame({'VIAS': visa, 'M': macy, 'AAPL': apple, 'T': att}, index=data_index)

    @property
    def df(self):
        """
        Property getter
        :return: Property value
        """
        return self._df

    @property
    def prof_df(self):
        """
        Property getter
        :return: Property value
        """
        return self._prof_df

    @property
    def portfolio_size(self):
        """
        Property getter
        :return: Property value
        """
        if self._prof_df is None:
            return len(self._df.columns)
        elif len(self._prof_df.columns) == 0:
            return len(self._df.columns)
        else:
            return len(self._prof_df.columns)

    def profitability(self) -> pd.DataFrame:
        """
        Calculates profitability for set data frame
        :return: The dataframe with profitability values.
        """
        return self._df.div(self.df.shift(1)).apply(np.log).loc[self._df.index.isin(list(self._df.index)[1:])]

    def exclude_loss(self) -> None:
        """
        Excludes loss positions from the dataframe
        :return: None
        """
        prof = self.profitability()
        prof.fillna(0, inplace=True)
        filter_tickers = self.get_total_profitability(prof)
        filter_tickers = filter_tickers.loc[filter_tickers > 0].copy()
        self._df = self._df[list(filter_tickers.index)]
        self._prof_df = self.profitability().fillna(0)

    def get_portfolio_profitability(self, shares) -> float:
        """
        Calculates portfolio profitability with given shares
        :param shares: given shares of actives in the portfolio
        :return: overall profitability
        """
        # check length of shares' vector
        if len(shares) != len(self._df.columns):
            raise ValueError("Incorrect share's vector length")

        # check shares sum. Normalizes it if necessary
        if shares.sum() != 1:
            shares = shares / np.linalg.norm(shares, ord=1)

        # main calculation
        np_shares = np.array(shares)
        np_profs = np.array(self.get_total_profitability(self._prof_df))
        return float(np.dot(np_shares, np_profs))

    def get_portfolio_risk(self, shares) -> float:
        """
        Calculates overall portfolio risk
        :param shares: given shares of actives in the portfolio
        :return: overall risk
        """

        if len(shares) != len(self._df.columns):
            raise ValueError("Incorrect share's vector length")
        np_shares = np.array(shares)
        np_cov = self.covariance(self._prof_df)
        prod_1 = np.dot(np_shares, np_cov)
        return float(np.sqrt(np.dot(prod_1, np_shares)))

    def generate_shares(self, strategy=None) -> np.ndarray:
        """
        Generates shares due to given strategy
        :param strategy: None(default)/growth/diversification
        :return: NumPy array with shares
        """
        if strategy is None:
            high_limit = 0.15
        elif strategy == 'growth':
            high_limit = 0.25
        elif strategy == 'diversification':
            high_limit = 0.07
        else:
            raise ValueError('Incorrect strategy type')

        shares = np.zeros(self.portfolio_size)
        shares[0] = 1
        count = 0
        while np.any(shares > high_limit):
            shares = [randrange(0, int(high_limit * 100)) for _ in range(self.portfolio_size)]
            shares = shares / np.linalg.norm(shares, ord=1)
            count += 1
            if count > self.ITERATION_LIMIT:
                raise RuntimeError('Too many iterations')
        return shares

    def generate_portfolios(self, n, strategy=None, profitability=0, risk=0):
        """
        Generates portfolios for given criteria.
        Only one criteria should be given. Otherwise a ValueError is raised
        :param n: Number of sample portfolios to return
        :param strategy: Chosen strategy for shares NumPy array
        :param profitability: Profitability low limit
        :param risk: Risk high limit
        :return: list of sample portfolio with the given length
        """
        if ((profitability != 0) & (risk != 0)) | ((profitability == 0) & (risk == 0)):
            raise ValueError('Improper selection conditions')

        portfolios = []
        while len(portfolios) < n:
            shares = self.generate_shares(strategy=strategy)
            if profitability != 0:
                if profitability < self.get_portfolio_profitability(shares):
                    portfolios.append(shares)
            else:
                if risk > self.get_portfolio_risk(shares):
                    portfolios.append(shares)

        return portfolios

    @staticmethod
    def covariance(df) -> np.ndarray:
        """
        Covariance calculation.
        Can be changed on a standard method from NumPy or Pandas
        :param df: A dataframe with data. The covariance is calculated via columns
        :return: covariance matrix in the NumPy format
        """
        n = len(df.columns)
        matrix = np.zeros((n, n))

        for i, first in enumerate(list(df.columns)):
            for j, second in enumerate(list(df.columns)):
                x = np.array(df[first])
                y = np.array(df[second])
                mean_x = x.mean() * np.ones(len(x))
                mean_y = y.mean() * np.ones(len(y))
                delta_x = mean_x - x
                delta_y = mean_y - y
                matrix[i, j] = 1 / len(x) * np.sum(np.multiply(delta_x, delta_y))

        return matrix

    @staticmethod
    def get_total_profitability(df) -> pd.Series:
        """
        Total profitability via tickers (columns sum in fact)
        :param df: Dataframe with data.
        :return: Pandas Series with total profitability via tickers.
        """
        return df.apply(np.mean, axis=0)

    @staticmethod
    def get_risk(df) -> np.ndarray:
        """
        Risk calculation. In fact - standard deviation.
        :param df: Dataframe with data.
        :return: NumPy array with risks via tickers
        """
        return df.std()
