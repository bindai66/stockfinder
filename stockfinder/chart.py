#-*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from . import core

def number_formatter(y, pos):
	return '{:,.0f}'.format(y)

def draw_basic_chart(symbol, start_date=None):
	if not start_date:
		start_date = '19000101'

	conn = core.get_connection()
	cur = conn.cursor()
	cur.execute('''select a.date
	                    , a.close, b.p5, b.p10, b.p20
	                    , a.volume, b.v5, b.v10, b.v20
	                 from ohlcv a left outer join ind_ma b on
	                          (a.symbol = b.symbol and a.date = b.date)
	                where a.symbol = ?
	                  and a.date >= ?
	                 order by a.date''', (symbol, start_date, ))

	vals = None
	for row in cur:
		if not vals:
			vals = []
			for i in range(len(row)):
				vals.append([])

		for i in range(len(row)):
			vals[i].append(row[i])
	cur.close()
	conn.close()

	fig, ax = plt.subplots(nrows=2)
	ax[0].set_title('Stock Finder [{}]'.format(symbol))

	# Prices
	ax[0].plot(vals[0], vals[1], '.-', label='Close')
	ax[0].plot(vals[0], vals[2], ':', label='MA-5')
	ax[0].plot(vals[0], vals[3], ':', label='MA-10')
	ax[0].plot(vals[0], vals[4], ':', label='MA-20')

	date_ticks = vals[0][::-int(len(vals[0]) / 12)]
	ax[0].set_xticks(date_ticks)
	ax[0].yaxis.set_major_formatter(FuncFormatter(number_formatter))

	ax[0].set_axisbelow(True)
	ax[0].xaxis.grid(True, color='gray', linestyle='dashed', linewidth=0.5)
	ax[0].yaxis.grid(True, color='gray', linestyle='dashed', linewidth=0.5)

	ax[0].set_ylabel('Prices')
	ax[0].legend()

	# Volumes
	ax[1].bar(vals[0], vals[5], label='Volume')
	ax[1].plot(vals[0], vals[6], ':', label='MA-5')
	ax[1].plot(vals[0], vals[7], ':', label='MA-10')
	ax[1].plot(vals[0], vals[8], ':', label='MA-20')

	date_ticks = vals[0][::-int(len(vals[0]) / 12)]
	ax[1].set_xticks(date_ticks)
	ax[1].yaxis.set_major_formatter(FuncFormatter(number_formatter))

	ax[1].set_axisbelow(True)
	ax[1].xaxis.grid(True, color='gray', linestyle='dashed', linewidth=0.5)
	ax[1].yaxis.grid(True, color='gray', linestyle='dashed', linewidth=0.5)

	ax[1].set_xlabel('Date')
	ax[1].set_ylabel('Volumes')
	ax[1].legend()
