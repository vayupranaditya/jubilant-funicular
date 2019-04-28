import moz_sql_parser
import json

def parse(query):
	try:
		sql = moz_sql_parser.parse(query)
	except:
		return 'Error'
	cols = []
	tabs = []
	joins = []
	res = {}
	for col in sql['select']:
		if 'value' in col:
			cols.append(col['value'])
		else:
			cols.append(col)
	res['columns'] = cols

	if sql['from'].__class__.__name__ == 'list':
		for tab in sql['from']:
			if 'join' in tab:
				tabs.append(tab['join'])
				if list(tab['on'].keys())[0] == 'eq':
					sign = '='
				elif list(tab['on'].keys())[0] == 'lt':
					sign = '<'
				elif list(tab['on'].keys())[0] == 'lte':
					sign = '<='
				elif list(tab['on'].keys())[0] == 'gt':
					sign = '>'
				elif list(tab['on'].keys())[0] == 'gte':
					sign = '>='
				else:
					sign = '<>'
				on = list(tab['on'].values())[0][0] + ' ' + sign + ' ' + list(tab['on'].values())[0][1]
				joins.append({'table' : tab['join'], 'condition' : on})
				res['joins'] = joins
			else:
				tabs.append(tab)
	else:
		tabs = sql['from']

	res['tables'] = tabs

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