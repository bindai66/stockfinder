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
_conn = None

MARKET_KOSPI = '1'
MARKET_KOSDAQ = '2'

# ==============================================================================
def get_connection(reload=False):
	global _conn

	if reload:
		if _conn:
			_conn.close()
		_conn = None

	if _conn == None:
		dbpath = _context.get('db_path', ':memory:')
		_conn = sqlite3.connect(dbpath)
	return _conn

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

	conn = get_connection(True)
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

	conn = get_connection()
	cur = conn.cursor()
	cur.execute('''SELECT SYMBOL
	                 FROM BASE_ITEM''')
	symbols = list(map(lambda x: x[0], cur.fetchall()))
	cur.close()

	df = get_krx_rank(date)
	vals = []
	for i in range(len(df)):
		if df['symbol'][i] in symbols:
			d = [df['symbol'][i], date, int(df['open'][i]), int(df['high'][i]),
				int(df['low'][i]), int(df['close'][i]), int(df['vol'][i]),
				float(df['foreign'][i])]
			vals.append(d)

	conn = get_connection()
	cur = conn.cursor()
	cur.executemany('''INSERT INTO OHLCV VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', vals)
	cur.close()
	conn.commit()

	end = time.time()
	elapsed = end - begin
	logging.debug('FILL KRX RANK. DATE: {}, ELAPSED: {:.3f}'.format(date, elapsed))

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

	conn = get_connection()
	cur = conn.cursor()
	cur.executemany('''INSERT INTO BASE_ITEM VALUES (?, ?, ?, ?, ?, ?, ?)''', vals)
	cur.close()
	conn.commit()

	end = time.time()
	elapsed = end - begin
	logging.debug('FILL KRX BASE. MARKET: {}, ELAPSED: {:.3f}'.format(market, elapsed))

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

	conn = get_connection()
	cur = conn.cursor()
	cur.executemany('''INSERT INTO CALENDAR VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
		dates)
	cur.close()
	conn.commit()

	end = time.time()
	elapsed = end - begin
	logging.debug('FILL CALENDAR. YEAR: {}, ELAPSED: {:.3f}'.format(year, elapsed))

# ==============================================================================
def adjust_df(df):
	old = pd.options.mode.chained_assignment
	pd.options.mode.chained_assignment = None
	for colname in ['OPEN', 'HIGH', 'LOW']:
		idx = df[colname] == 0
		df[colname][idx] = df['CLOSE'][idx]

	# 삼성전자 액면분할 조정
	if len(df) > 0 and df['SYMBOL'][0] == '005930':
		idx = df['DATE'] <= '20180503'
		for colname in ['OPEN', 'HIGH', 'LOW', 'CLOSE']:
			df[colname][idx] = round(df[colname][idx] / 50)

	pd.options.mode.chained_assignment = old

# ==============================================================================
def fill_indices_ma(df):
	if len(df) <= 0:
		return

	conn = get_connection()
	cur = conn.cursor()
	cur.execute('SELECT MAX(DATE) FROM IND_MA WHERE SYMBOL = ?', (df['SYMBOL'][0], ))
	lastDate = cur.fetchone()[0]
	cur.close()

	if not lastDate:
		lastDate = '19000101'

	vals = []
	for i in range(len(df)):
		if df['DATE'][i] <= lastDate:
			break

		ma = [df['SYMBOL'][i], df['DATE'][i]]
		ma_p = []
		ma_v = []
		for period in [5, 10, 20, 60]:
			begin = i
			end = min(i + period, len(df))
			ma_p.append(round(sum(df['CLOSE'][begin:end]) / len(df['CLOSE'][begin:end])))
			ma_v.append(round(sum(df['VOLUME'][begin:end]) / len(df['VOLUME'][begin:end])))
		ma.extend(ma_p)
		ma.extend(ma_v)
		vals.append(ma)

	conn = get_connection()
	cur = conn.cursor()
	cur.executemany('''INSERT INTO IND_MA VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', vals)
	cur.close()
	conn.commit()

# ==============================================================================
def fill_indices_macd(df):
	pass

# ==============================================================================
def fill_indices_rsi(df):
	pass

# ==============================================================================
def fill_indices_stochastic(df):
	pass

# ==============================================================================
def fill_indices_williams_r(df):
	pass

# ==============================================================================
def fill_indices():
	conn = get_connection()
	cur = conn.cursor()
	cur.execute('SELECT SYMBOL FROM BASE_ITEM')
	symbols = list(map(lambda x: x[0], cur.fetchall()))

	elapsed = 0
	total = 0
	count = 0
	for symbol in symbols:
		begin = time.time()
		df = pd.read_sql_query('''SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME
		                          FROM OHLCV
		                          WHERE SYMBOL = ?
		                          ORDER BY DATE DESC
		                       ''', conn, params=(symbol, ))
		adjust_df(df)
		fill_indices_ma(df)
		fill_indices_macd(df)
		fill_indices_rsi(df)
		fill_indices_stochastic(df)
		fill_indices_williams_r(df)

		end = time.time()
		elapsed = end - begin
		logging.debug('GENERATE INDICES. SYMBOL: {}, ELAPSED: {:.3f}'.format(symbol, elapsed))
