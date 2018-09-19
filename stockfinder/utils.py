#-*- coding: utf-8 -*-

def parse_int(s, defval = 0):
    try:
        return int(s.replace(',', ''))
    except:
        return defval

def parse_float(s, defval = 0.0):
    try:
        return float(s.replace(',', ''))
    except:
        return defval