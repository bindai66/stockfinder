#-*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.finance as matfin
from matplotlib.ticker import FuncFormatter

from . import core

def draw_basic_chart(symbol, start_date=None):
	def number_formatter(y, pos):
		return '{:,.0f}'.format(y)

	def default_axis_set(ax, x_ticks, label, **kargs):
		ax.set_xticks(x_ticks)
		ax.yaxis.set_major_formatter(FuncFormatter(number_formatter))
		ax.set_axisbelow(True)
		ax.xaxis.grid(True, color='gray', linestyle='dashed', linewidth=0.5)
		ax.yaxis.grid(True, color='gray', linestyle='dashed', linewidth=0.5)
		ax.set_ylabel(label)
		if 'ylim' in kargs:
			ax.set_ylim(kargs['ylim'])
		ax.legend()

	if not start_date:
		start_date = '19000101'

	with core.DBConn() as conn:
		cur = conn.cursor()
		cur.execute('''SELECT A.DATE
		                    , A.OPEN, A.HIGH, A.LOW, A.CLOSE, B.P5, B.P10, B.P20  -- [1, 7]
		                    , A.VOLUME, B.V5, B.V10, B.V20 -- [8, 11]
		                    , C.MACD, C.SIGNAL, C.HIST     -- [12, 14]
		                    , D.K, D.D                     -- [15, 16]
		                    , E.WIL_R                      -- [17]
		                    , F.RSI                        -- [18]
		                 FROM OHLCV A
                                 LEFT OUTER JOIN IND_MA B ON (A.SYMBOL = B.SYMBOL AND A.DATE = B.DATE)
                                 LEFT OUTER JOIN IND_MACD C ON (A.SYMBOL = C.SYMBOL AND A.DATE = C.DATE)
                                 LEFT OUTER JOIN IND_STOCHASTIC D ON (A.SYMBOL = D.SYMBOL AND A.DATE = D.DATE)
                                 LEFT OUTER JOIN IND_WIL_R E ON (A.SYMBOL = E.SYMBOL AND A.DATE = E.DATE)
                                 LEFT OUTER JOIN IND_RSI F ON (A.SYMBOL = F.SYMBOL AND A.DATE = F.DATE)
		                WHERE A.SYMBOL = ?
		                  AND A.DATE >= ?
		                 ORDER BY A.DATE''', (symbol, start_date, ))

		vals = None
		for row in cur:
			if not vals:
				vals = [[] for i in range(len(row))]

			for i in range(len(row)):
				vals[i].append(row[i])
		cur.close()
		conn.close()

	_, ax = plt.subplots(nrows=6, gridspec_kw = { 'height_ratios': [2, 1, 1, 1, 1, 1]})
	ax[0].set_title('Stock Finder [{}]'.format(symbol))
	ax[-1].set_xlabel('Date')
	date_ticks = vals[0][::-int(len(vals[0]) / 12)]

	# Prices
	matfin.candlestick2_ohlc(ax[0], vals[1], vals[2], vals[3], vals[4], 
			width=0.6, colorup='r', colordown='b')
	ax[0].plot(vals[0], vals[1], '--', label='Close')
	ax[0].plot(vals[0], vals[5], ':', label='MA-5')
	ax[0].plot(vals[0], vals[6], ':', label='MA-10')
	ax[0].plot(vals[0], vals[7], ':', label='MA-20')
	default_axis_set(ax[0], date_ticks, 'Prices')

	# Volumes
	ax[1].bar(vals[0], vals[8], label='Volume')
	ax[1].plot(vals[0], vals[9], ':', label='MA-5')
	ax[1].plot(vals[0], vals[10], ':', label='MA-10')
	ax[1].plot(vals[0], vals[11], ':', label='MA-20')
	default_axis_set(ax[1], date_ticks, 'Volumes')

	# MACD
	ax[2].plot(vals[0], vals[12], ':', label='MACD')
	ax[2].plot(vals[0], vals[13], ':', label='SIGNAL')
	ax[2].bar(vals[0], vals[14], label='HIST')
	default_axis_set(ax[2], date_ticks, 'MACD')

	# STOCHASTIC
	ax[3].plot(vals[0], vals[15], '-', label='Fast%K')
	ax[3].plot(vals[0], vals[16], '-', label='Fast%D')
	ax[3].plot(vals[0], [20] * len(vals[0]), 'r:', label='')
	ax[3].plot(vals[0], [80] * len(vals[0]), 'r:', label='')
	default_axis_set(ax[3], date_ticks, 'STOCHASTIC', ylim=[0, 100])

	# WILLIAM %R
	ax[4].plot(vals[0], vals[17], '-', label='William%R')
	ax[4].plot(vals[0], [-20] * len(vals[0]), 'r:', label='')
	ax[4].plot(vals[0], [-80] * len(vals[0]), 'r:', label='')
	default_axis_set(ax[4], date_ticks, 'WILLIAM %R', ylim=[-100, 0])

	# RSI
	ax[5].plot(vals[0], vals[18], '-', label='RSI')
	ax[5].plot(vals[0], [30] * len(vals[0]), 'r:', label='')
	ax[5].plot(vals[0], [70] * len(vals[0]), 'r:', label='')
	default_axis_set(ax[5], date_ticks, 'RSI', ylim=[0, 100])


