#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from app.portfolio import Portfolio
from random import randrange
import numpy as np


class PortfolioTestCase(unittest.TestCase):

    HIGH_LIMIT = 25

    def setUp(self) -> None:
        self.portfolio = Portfolio()
        self.portfolio.load_test_data()

    def test_portfolio(self):
        self.portfolio.exclude_loss()
        shares = [randrange(0, self.HIGH_LIMIT) for _ in range(self.portfolio.portfolio_size)]
        shares = shares / np.linalg.norm(shares, ord=1)
        self.assertAlmostEqual(1, float(np.sum(shares)), 2)
        print(self.portfolio.get_portfolio_profitability(shares))
        print(self.portfolio.get_portfolio_risk(shares))

    def test_shares_generator(self):
        self.portfolio.exclude_loss()
        self.assertTrue(np.all(self.portfolio.generate_shares(strategy='growth') < 0.25))
        self.assertRaises(RuntimeError, self.portfolio.generate_shares)
        self.assertRaises(ValueError, self.portfolio.generate_shares, 'foo')

    def test_portfolios_generator(self):
        self.portfolio.exclude_loss()
        result = self.portfolio.generate_portfolios(5, strategy='growth', risk=0.1)
        for shares_set in result:
            self.assertAlmostEqual(1, sum(shares_set), 2)
