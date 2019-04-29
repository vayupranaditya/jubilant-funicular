import moz_sql_parser
import json
import math

def isColExist(col, tabs, datatable):
	if col == '*':
		return True
	if '.' in col:
		datas = col.split('.')
		return datas[1] in datatable[datas[0]]['tabel']
	try:
		if tabs.__class__.__name__ == 'list':
			for tab in tabs:
				if col in datatable[tab]['tabel']:
					return True
		else:
			return col in datatable[tabs]['tabel']
	except:
		return False
	return False

def readFile(name):
	with open(name) as json_file:
		return json.load(json_file)


def parse(query):
	try:
		sql = moz_sql_parser.parse(query)
	except:
		return 'Syntax error'
	cols = []
	tabs = []
	joins = []
	res = {}

	data = readFile('data-dictionary.json')
	keys = list(data.keys())[2:]
	vals = list(data.values())[2:]
	data = {}
	for i in range(0, len(keys)):
		data[keys[i]] = vals[i]

	# print(sql['select'].__class__.__name__)
	if sql['select'].__class__.__name__ == 'list':
		for col in sql['select']:
			if 'value' in col:
				cols.append(col['value'])
			else:
				cols.append(col)
	else:
		cols = sql['select']['value']
	res['columns'] = cols

	if sql['from'].__class__.__name__ == 'list':
		for tab in sql['from']:
			if 'join' in tab:
				tabs.append(tab['join'])
				joins.append({'table' : tab['join'], 'using' : tab['using']})
			else:
				tabs.append(tab)
	else:
		tabs = sql['from']

	res['tables'] = tabs
	res['joins'] = joins

	if cols.__class__.__name__ == 'list':
		for col in cols:
			if not isColExist(col, tabs, data):
				return 'Unknown column '+ col
	else:
		if not isColExist(cols, tabs, data):
			return 'Unknown column '+ col

	condition = ''
	if 'where' in sql:
		if 'eq' in sql['where']:
			if sql['where']['eq'][1].__class__.__name__ == 'dict':
				condition = sql['where']['eq'][0] + ' = \'' + sql['where']['eq'][1]['literal'] + '\''
			else:
				condition = str(sql['where']['eq'][0]) + ' = ' + str(sql['where']['eq'][1])
		elif 'gt' in sql['where']:
			if sql['where']['gt'][1].__class__.__name__ == 'dict':
				condition = sql['where']['gt'][0] + ' > \'' + sql['where']['gt'][1]['literal'] + '\''
			else:
				condition = str(sql['where']['gt'][0]) + ' > ' + str(sql['where']['gt'][1])
		elif 'gte' in sql['where']:
			if sql['where']['gte'][1].__class__.__name__ == 'dict':
				condition = sql['where']['gte'][0] + ' >= \'' + sql['where']['gte'][1]['literal'] + '\''
			else:
				condition = str(sql['where']['gte'][0]) + ' >= ' + str(sql['where']['gte'][1])
		elif 'lt' in sql['where']:
			if sql['where']['lt'][1].__class__.__name__ == 'dict':
				condition = sql['where']['lt'][0] + ' < \'' + sql['where']['lt'][1]['literal'] + '\''
			else:
				condition = str(sql['where']['lt'][0]) + ' < ' + str(sql['where']['lt'][1])
		elif 'lte' in sql['where']:
			if sql['where']['lte'][1].__class__.__name__ == 'dict':
				condition = sql['where']['lte'][0] + ' <= \'' + sql['where']['lte'][1]['literal'] + '\''
			else:
				condition = str(sql['where']['lte'][0]) + ' <= ' + str(sql['where']['lte'][1])
		else:
			if sql['where']['neq'][1].__class__.__name__ == 'dict':
				condition = sql['where']['neq'][0] + ' <> \'' + sql['where']['neq'][1]['literal'] + '\''
			else:
				condition = str(sql['where']['neq'][0]) + ' <> ' + str(sql['where']['neq'][1])
	res['conditions'] = condition
	return res