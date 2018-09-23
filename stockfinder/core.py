#-*- coding: utf-8 -*-

import csv
import os.path
import sqlite3
import time
import logging

import numpy as np
import pandas as pd

from . import config
from . import utils

_context = config.context

MARKET_KOSPI = '1'
MARKET_KOSDAQ = '2'

# ==============================================================================
class DBConn:

	def __init__(self):
		dbpath = _context.get('db_path', ':memory:')
		self._conn = sqlite3.connect(dbpath)

	def __enter__(self):
		return self._conn

	def __exit__(self, exec_type, exec_value, traceback):
		self._conn.close()

	def get_connection(self):
		return self._conn

# ==============================================================================
def init_database(forcelyClear=False):
	if forcelyClear:
		dbpath = _context.get('db_path', None)
		if dbpath:
			try:
				os.unlink(dbpath)
			except:
				pass

	res_path = os.path.join('res', 'init.sql')
	with open(res_path, 'r', encoding='utf-8') as f:
		sql_text = f.read()

	with DBConn() as conn:
		cur = conn.cursor()
		cur.executescript(sql_text)
		cur.close()

# ==============================================================================
def get_krx_file(filepath, layout):
	vals = []
	max_idx = max(map(lambda x: x[0], layout))

	with open(filepath, 'r', encoding='utf-8') as f:
		# drop the header line
		f.readline()

		reader = csv.reader(f)
		for r in reader:
			if len(r) < max_idx:
				continue

			v = {}
			for l in layout:
				if l[2] == int:
					v[l[1]] = utils.parse_int(r[l[0]])
				elif l[2] == float:
					v[l[1]] = utils.parse_float(r[l[0]])
				else:
					v[l[1]] = r[l[0]]
			vals.append(v)

	cols = list(map(lambda x: x[1], layout))
	return pd.DataFrame(vals, columns=cols)

# ==============================================================================
def get_krx_rank(date):
	filepath = os.path.join('data', 'data_{}.csv'.format(date))
	layout = [
		(1, 'symbol', str),
		(2, 'name', str),
		(8, 'open', int),
		(9, 'high', int),
		(10, 'low', int),
		(3, 'close', int),
		(6, 'vol', int),
		(15, 'foreign', float),
	]
	return get_krx_file(filepath, layout)

# ==============================================================================
def fill_krx_rank(date):
	begin = time.time()
	with DBConn() as conn:

		cur = conn.cursor()
		cur.execute('''SELECT SYMBOL
						 FROM BASE_ITEM''')
		symbols = list(map(lambda x: x[0], cur.fetchall()))
		cur.close()

		df = get_krx_rank(date)
		vals = []
		for i in range(len(df)):
			if df['symbol'][i] in symbols:
				# 거래 없는 것들 보정 (거래정지)
				for k in ['open', 'high', 'low']:
					if df[k][i] == 0:
						df[k][i] = df['close'][i]

				# 삼성전자 감자 보정
				if date <= '20180503' and df['symbol'][i] == '005930':
					for k in ['open', 'high', 'low', 'close']:
						df[k][i] = round(df[k][i] / 50)

				d = [df['symbol'][i], date, int(df['open'][i]), int(df['high'][i]),
					int(df['low'][i]), int(df['close'][i]), int(df['vol'][i]),
					float(df['foreign'][i])]
				vals.append(d)

		cur = conn.cursor()
		cur.executemany('''INSERT INTO OHLCV VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', vals)
		cur.close()
		conn.commit()

	end = time.time()
	elapsed = end - begin
	print('FILL KRX RANK. DATE: {}, ELAPSED: {:.3f}'.format(date, elapsed))

# ==============================================================================
def get_krx_base(market):
	if market == MARKET_KOSPI:
		filepath = os.path.join('data', 'base_kospi.csv')
	elif market == MARKET_KOSDAQ:
		filepath = os.path.join('data', 'base_kosdaq.csv')
	else:
		raise Exception('Invalid market code')

	layout = [
		(1, 'symbol', str),
		(2, 'name', str),
		(3, 'bizcode', str),
		(4, 'bizname', str),
		(5, 'lst_qty', int),
		(6, 'capital', int),
	]
	return get_krx_file(filepath, layout)

# ==============================================================================
def fill_krx_base(market):
	begin = time.time()

	df = get_krx_base(market)
	vals = []
	for i in range(len(df)):
		d = [df['symbol'][i], df['name'][i], market, df['bizcode'][i],
			df['bizname'][i], int(df['lst_qty'][i]), int(df['capital'][i])]
		vals.append(d)

	with DBConn() as conn:
		cur = conn.cursor()
		cur.executemany('''INSERT INTO BASE_ITEM VALUES (?, ?, ?, ?, ?, ?, ?)''', vals)
		cur.close()
		conn.commit()

	end = time.time()
	elapsed = end - begin
	print('FILL KRX BASE. MARKET: {}, ELAPSED: {:.3f}'.format(market, elapsed))

# ==============================================================================
def fill_calendar(year):
	def get_holiday():
		days = []
		filepath = os.path.join('data', 'holiday.txt')
		with open(filepath, 'r') as f:
			for line in f:
				d = line.strip()
				if len(d) == 8:
					days.append(d)
		return days

	def is_holiday(d):
		tm = time.strptime(d, '%Y%m%d')
		return tm.tm_wday in [5, 6] or d in holidays

	begin = time.time()

	holidays = get_holiday()
	day_t = 24 * 60 * 60
	begin_t = time.mktime(time.strptime('{}0101'.format(year), '%Y%m%d'))
	end_t = time.mktime(time.strptime('{}1231'.format(year), '%Y%m%d'))
	cur_t = begin_t
	date_no = 0
	bizdate_no = 0
	dates = []
	while cur_t <= end_t:
		cur_tm = time.localtime(cur_t)
		cur_date = time.strftime('%Y%m%d', cur_tm)
		work = is_holiday(cur_date) and 'N' or 'Y'
		prev_date = time.strftime('%Y%m%d', time.localtime(cur_t - day_t))
		next_date = time.strftime('%Y%m%d', time.localtime(cur_t + day_t))

		prev_bizdate = prev_date
		delta = 2
		while is_holiday(prev_bizdate):
			tm = time.localtime(cur_t - (day_t * delta))
			prev_bizdate = time.strftime('%Y%m%d', tm)
			delta += 1

		next_bizdate = next_date
		delta = 2
		while is_holiday(next_bizdate):
			tm = time.localtime(cur_t + (day_t * delta))
			next_bizdate = time.strftime('%Y%m%d', tm)
			delta += 1

		date_no += 1
		bizdate_no += work == 'Y' and 1 or 0
		cur_t += day_t

		dates.append((cur_date, work, prev_date, next_date, prev_bizdate,
			next_bizdate, date_no, bizdate_no, ))

	with DBConn() as conn:
		cur = conn.cursor()
		cur.executemany('''INSERT INTO CALENDAR VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
			dates)
		cur.close()
		conn.commit()

	end = time.time()
	elapsed = end - begin
	print('FILL CALENDAR. YEAR: {}, ELAPSED: {:.3f}'.format(year, elapsed))

# ==============================================================================
def fill_indices_ma(df, conn):
	if len(df) <= 0:
		return

	cur = conn.cursor()
	cur.execute('SELECT MAX(DATE) FROM IND_MA WHERE SYMBOL = ?', (df['SYMBOL'][0], ))
	lastDate = cur.fetchone()[0]
	cur.close()

	if not lastDate:
		lastDate = '19000101'

	p5 = df['CLOSE'].rolling(window=5).mean()
	p10 = df['CLOSE'].rolling(window=10).mean()
	p20 = df['CLOSE'].rolling(window=20).mean()
	p60 = df['CLOSE'].rolling(window=60).mean()
	v5 = df['VOLUME'].rolling(window=5).mean()
	v10 = df['VOLUME'].rolling(window=10).mean()
	v20 = df['VOLUME'].rolling(window=20).mean()
	v60 = df['VOLUME'].rolling(window=60).mean()

	vals = []
	for i in range(len(df)):
		if df['DATE'][i] <= lastDate:
			continue

		vals.append([df['SYMBOL'][i], df['DATE'][i],
			p5[i], p10[i], p20[i], p60[i], v5[i], v10[i], v20[i], v60[i]])

	cur = conn.cursor()
	cur.executemany('''INSERT INTO IND_MA VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', vals)
	cur.close()
	conn.commit()

# ==============================================================================
def fill_indices_ema(df, conn):
	if len(df) <= 0:
		return

	cur = conn.cursor()
	cur.execute('SELECT MAX(DATE) FROM IND_EMA WHERE SYMBOL = ?', (df['SYMBOL'][0], ))
	lastDate = cur.fetchone()[0]
	cur.close()

	if not lastDate:
		lastDate = '19000101'

	p5 = df['CLOSE'].ewm(span=5).mean()
	p10 = df['CLOSE'].ewm(span=10).mean()
	p20 = df['CLOSE'].ewm(span=20).mean()
	p60 = df['CLOSE'].ewm(span=60).mean()
	v5 = df['VOLUME'].ewm(span=5).mean()
	v10 = df['VOLUME'].ewm(span=10).mean()
	v20 = df['VOLUME'].ewm(span=20).mean()
	v60 = df['VOLUME'].ewm(span=60).mean()

	vals = []
	for i in range(len(df)):
		if df['DATE'][i] <= lastDate:
			continue

		vals.append([df['SYMBOL'][i], df['DATE'][i],
			p5[i], p10[i], p20[i], p60[i], v5[i], v10[i], v20[i], v60[i]])

	cur = conn.cursor()
	cur.executemany('''INSERT INTO IND_EMA VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', vals)
	cur.close()
	conn.commit()

# ==============================================================================
def fill_indices_macd(df, conn):
	if len(df) <= 0:
		return

	cur = conn.cursor()
	cur.execute('SELECT MAX(DATE) FROM IND_MACD WHERE SYMBOL = ?', (df['SYMBOL'][0], ))
	lastDate = cur.fetchone()[0]
	cur.close()

	if not lastDate:
		lastDate = '19000101'

	ma_12 = df['CLOSE'].ewm(span=12).mean()
	ma_26 = df['CLOSE'].ewm(span=26).mean()
	macd = ma_12 - ma_26
	signal = macd.ewm(span=9).mean()
	hist = macd - signal
	vals = []
	for i in range(len(df)):
		if df['DATE'][i] <= lastDate:
			continue

		vals.append([df['SYMBOL'][i], df['DATE'][i],
			ma_12[i], ma_26[i], macd[i], signal[i], hist[i]])

	cur = conn.cursor()
	cur.executemany('''INSERT INTO IND_MACD VALUES (?, ?, ?, ?, ?, ?, ?)''', vals)
	cur.close()
	conn.commit()

# ==============================================================================
def fill_indices_rsi(df, conn):
	if len(df) <= 0:
		return

	cur = conn.cursor()
	cur.execute('SELECT MAX(DATE) FROM IND_RSI WHERE SYMBOL = ?', (df['SYMBOL'][0], ))
	lastDate = cur.fetchone()[0]
	cur.close()

	if not lastDate:
		lastDate = '19000101'

	up = np.where(df['CLOSE'].diff(1) > 0, df['CLOSE'].diff(1), 0)
	dn = np.where(df['CLOSE'].diff(1) < 0, df['CLOSE'].diff(1) * -1, 0)

	avg_up = pd.Series(up).rolling(7).mean()
	avg_dn = pd.Series(dn).rolling(7).mean()
	rs = avg_up / avg_dn
	rsi = rs / (1. + rs) * 100

	vals = []
	for i in range(len(df)):
		if df['DATE'][i] <= lastDate:
			continue

		vals.append([df['SYMBOL'][i], df['DATE'][i], rsi[i]])

	cur = conn.cursor()
	cur.executemany('''INSERT INTO IND_RSI VALUES (?, ?, ?)''', vals)
	cur.close()
	conn.commit()

# ==============================================================================
def fill_indices_stochastic(df, conn):
	if len(df) <= 0:
		return

	cur = conn.cursor()
	cur.execute('SELECT MAX(DATE) FROM IND_STOCHASTIC WHERE SYMBOL = ?', (df['SYMBOL'][0], ))
	lastDate = cur.fetchone()[0]
	cur.close()

	if not lastDate:
		lastDate = '19000101'

	high = df['HIGH'].rolling(5).max()
	low = df['LOW'].rolling(5).min()
	k = (df['CLOSE'] - low) / (high - low) * 100
	d = k.rolling(3).mean()

	vals = []
	for i in range(len(df)):
		if df['DATE'][i] <= lastDate:
			continue

		vals.append([df['SYMBOL'][i], df['DATE'][i], k[i], d[i]])

	cur = conn.cursor()
	cur.executemany('''INSERT INTO IND_STOCHASTIC VALUES (?, ?, ?, ?)''', vals)
	cur.close()
	conn.commit()

# ==============================================================================
def fill_indices_williams_r(df, conn):
	if len(df) <= 0:
		return

	cur = conn.cursor()
	cur.execute('SELECT MAX(DATE) FROM IND_WIL_R WHERE SYMBOL = ?', (df['SYMBOL'][0], ))
	lastDate = cur.fetchone()[0]
	cur.close()

	if not lastDate:
		lastDate = '19000101'

	high = df['HIGH'].rolling(14).max()
	low = df['LOW'].rolling(14).min()
	wil_r = (high - df['CLOSE']) / (high - low) * -100

	vals = []
	for i in range(len(df)):
		if df['DATE'][i] <= lastDate:
			continue

		vals.append([df['SYMBOL'][i], df['DATE'][i], wil_r[i]])

	cur = conn.cursor()
	cur.executemany('''INSERT INTO IND_WIL_R VALUES (?, ?, ?)''', vals)
	cur.close()
	conn.commit()

# ==============================================================================
def fill_indices():
	with DBConn() as conn:
		cur = conn.cursor()
		cur.execute('''SELECT SYMBOL FROM BASE_ITEM''')
		symbols = list(map(lambda x: x[0], cur.fetchall()))
		symbols = ['005930']

		elapsed = 0
		total = 0
		count = 0
		for symbol in symbols:
			begin = time.time()
			df = pd.read_sql_query('''SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME
									  FROM OHLCV
									  WHERE SYMBOL = ?
									  ORDER BY DATE
								   ''', conn, params=(symbol, ))
			fill_indices_ma(df, conn)
			fill_indices_ema(df, conn)
			fill_indices_macd(df, conn)
			fill_indices_rsi(df, conn)
			fill_indices_stochastic(df, conn)
			fill_indices_williams_r(df, conn)

			end = time.time()
			elapsed = end - begin
			print('GENERATE INDICES. SYMBOL: {}, ELAPSED: {:.3f}'.format(symbol, elapsed))
