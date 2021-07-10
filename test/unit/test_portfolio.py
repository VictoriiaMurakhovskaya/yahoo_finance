#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from app.portfolio import Portfolio
from datetime import datetime

unittest.TestLoader.sortTestMethodsUsing = None


class PortfolioTestCase(unittest.TestCase):
    SHARES = [0.25, 0.25, 0.5]

    def setUp(self) -> None:
        self.portfolio = Portfolio()
        self.portfolio.make_test_data()

    def test_data_load(self):
        self.portfolio.make_test_data()
        self.assertLess(0, len(self.portfolio.df))

    def test_ratio(self):
        prof = self.portfolio.profitability()
        self.assertAlmostEqual(0.029816, prof.at[list(prof.index)[-1], 'VIAS'], 2)

    def test_total_profitability(self):
        self.portfolio.exclude_loss()
        self.assertEqual(3, len(self.portfolio.df.columns))
        acquired = self.portfolio.get_total_profitability(self.portfolio.prof_df)
        self.assertAlmostEqual(0.0302, acquired[0], 2)
        self.assertAlmostEqual(0.0370, acquired[1], 2)
        self.assertAlmostEqual(0.0237, acquired[2], 2)

    def test_portfolio_profitability(self):
        self.portfolio.exclude_loss()
        self.assertAlmostEqual(0.0286, self.portfolio.get_portfolio_profitability(self.SHARES), 4)

    def test_portfolio_risk(self):
        self.portfolio.exclude_loss()
        self.assertAlmostEqual(0.0360, self.portfolio.get_portfolio_risk(self.SHARES), 3)



