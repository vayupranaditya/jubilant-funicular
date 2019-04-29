import math

def joinAr(obj):
	data = readFile('data-dictionary.json')
	ars = []
	ars.append([])
	projection = {'cols' : obj['columns'], 'algo': 'on the fly'}
	join = []
	bfrs = {}
	blocks = {}
	for tab in obj['tables']:
		bfrs[tab] = math.ceil(data['b']/data[tab]['r'])
		blocks[tab] = math.ceil(data[tab]['n']/bfrs[tab])
	join.append({
		'condition': obj['tables'][0]+'.'+obj['joins'][0]['using']+' = '+obj['tables'][1]+'.'+obj['joins'][0]['using'],
		'algo' : 'BNLJ',
		'tables': obj['tables'],
		'cost' : (blocks[obj['tables'][0]]*blocks[obj['tables'][1]])+blocks[obj['tables'][0]]
	})
	join.append({
		'condition': obj['tables'][1]+'.'+obj['joins'][0]['using']+' = '+obj['tables'][0]+'.'+obj['joins'][0]['using'],
		'algo' : 'BNLJ',
		'tables': [obj['tables'][1], obj['tables'][0]],
		'cost' : (blocks[obj['tables'][1]]*blocks[obj['tables'][0]])+blocks[obj['tables'][1]]
	})

	x = 1
	for j in join:
		print('QEP #'+str(x))
		print('PROJECTION', projection['cols'][0], '--', projection['algo'])
		for i in j['tables'][0]:
			print(' ', end='')
		print(j['condition'], '-- BNLJ')
		print(j['tables'][0],'    ', j['tables'][1])
		print('Cost (worst case):', j['cost'])
		print()
		x += 1

	print('QEP Optimal: #', str(1 if join[0]['cost'] < join[1]['cost'] else 2))
	if join[0]['cost'] < join[1]['cost']:
		return {
			'projection' : projection['cols'][0] + ' --' + projection['algo'],
			'join' : '     ' + join[0]['condition']+ '-- BNLJ',
			'tables' : join[0]['tables'][0]+'    '+ join[1]['tables'][1],
			'cost' : join[1]['cost']
		}
	else:
		return {
			'projection' : projection['cols'][0] + ' --' + projection['algo'],
			'join' : '     ' + join[1]['condition']+ '-- BNLJ',
			'tables' : join[1]['tables'][0]+'    '+ join[1]['tables'][1],
			'cost' : join[1]['cost']
		}