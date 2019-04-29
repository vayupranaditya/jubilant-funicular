
# coding: utf-8

# In[30]:


import json
import numpy
import math
import moz_sql_parser


# In[31]:


# isi data-dictionary.json 

#{
# 	"p" : 4,
# 	"b" : 8192,
# 	"pegawai" : {
# 		"tabel" : ["no_ktp", "tgl_lahir", "gender", "pendidikan"],
# 		"r" : 128,
# 		"n" : 10000,
# 		"v" : 12,
# 		"br" : 10000
# 	},
# 	"dirawat" : {
# 		"tabel" : ["tgl_dirawat", "status", "periode", "no_inventaris", "no_ktp"],
# 		"r" : 8,
# 		"n" : 150,
# 		"v" : 6,
# 		"br" : 1000
# 	},
# 	"fasilitas" : {
# 		"tabel" : ["no_inventaris", "nama", "jenis", "tgl_dibeli", "pemakaian"],
# 		"r" : 10,
# 		"n" : 1000,
# 		"v" : 7,
# 		"br" : 1000
# 	}
# }


# In[32]:


# read file
def readFile(name):
	with open(name) as json_file:
		return json.load(json_file)

data = readFile('data-dictionary.json')


# In[33]:


def fanoutRasio(tab):
    return math.floor(data['b'] / (data[tab]['v'] + data['p']))

def blockingFactor(tab):
    return math.floor(data['b'] / data[tab]['r'])

def jumBlokIndeks(r, t):
    if (r>data[t]['n']):
        return 'record tidak tersedia'
    else :
        fanout = fanoutRasio(t)
        return math.ceil(r / fanout)

def jumBlokNonIndeks(r, t):
    if (r>data[t]['n']):
        return 'record tidak tersedia'
    else :
        bfr = blockingFactor(t)
        return math.ceil(r / bfr)
    
def totalBlokData(tab):
    return math.ceil(data[tab]['n'] / blockingFactor(tab))

def totalBlokIndeks(tab):
    return math.ceil(data[tab]['n'] / fanoutRasio(tab))


# In[34]:


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

	print('QEP Optimal:', min(join[0]['cost'], join[1]['cost']))
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
    
    
#qep where
def whereAr(obj):
    data = readFile('data-dictionary.json')
    ars = []
    ars.append([])
    projection = {'cols' : obj['columns'], 'algo': 'on the fly'}
    where = []
    bfrs = {}
    blocks = {}
    for tab in obj['tables']:
        bfrs[tab] = math.ceil(data['b']/data[tab]['r'])
        blocks[tab] = math.ceil(data[tab]['n']/bfrs[tab])
    
    where.append({
        'condition': obj['tables'][0]+'.'+obj['joins'][0]['using']+' = '+obj['tables'][1]+'.'+obj['joins'][0]['using'],
        'algo' : 'A1 Non Key',
        'tables': obj['tables'],
        'cost' : (blocks[obj['tables'][0]]*blocks[obj['tables'][1]])+blocks[obj['tables'][0]]
    })
    where.append({
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

    print('QEP Optimal:', min(join[0]['cost'], join[1]['cost']))
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

# array_query = {}
# array_query['kolom'] = ['no_inventaris','nama']
# array_query['tabel'] = ['fasilitas']
# array_query['where'] = ['no_inventaris = 1']
# def qep(query):
#     if (query['kolom'][0] != '*'):
#         return { 
#             'projection' : query['kolom'],
#             'selection' : query['where'],
#             'tabel' : query['tabel']
#         }
#     else:
#         return {
#             'selection' : query['where'],
#             'tabel' : query['tabel']
#         }

# #cost where, diisi ya geng hehe
# def costA1NonKey(t):
#     return data[t]['br']

# def costA1Key(t):
#     return data[t]['br']/2

# def costA2(t):
#     return math.ceil(math.log(data[t]['br'],fanoutRasio(t))) + 1

# def costBNLJ(inner,outer):
#     return data[inner]['br']*data[outer]['br'] + data[outer]['br']

# #qep join, diisi ya geng hehe
# def whereQep(obj):
    
# #cost join, diisi ya geng hehe

# #tes qep where
# if(array_query['kolom']!='*'):
#     print('PROJECTION', qep(array_query)['projection'], ',' , qep(array_query)['projection'])
# print('SELECTION', qep(array_query)['selection'])
# print(qep(array_query)['tabel'])

# h = costA1key(array_query['tabel'])
# print(h)
# print(costBNLJ('fasilitas','pegawai'))


# In[35]:


def saveSharedPool(query, parse):
    if join in query:
        data_saved = joinAr(parse)
    else:
        data_saved = whereAr(parse)
        
    data_saved['query'] = query
    data = readFile('data-dictionary.json')
    data.append(data_saved)
    
    with open('shared-pool.json', 'w') as outfile:  
        json.dump(data, outfile)
        
def checkSharedPool(query):
    data_r = readFile('data-dictionary.json')
    return list(filter(lambda qry: qry['query'] == query, data_r))

def printSharedPool(query):
    data_r = readFile('data-dictionary.json')
    print(list(filter(lambda qry: qry['query'] == query, data_r)))


# In[36]:


def menuSatu():
    print('Menu 1 : BFR dan Fan Out Ratio')
    print('')
    
    print('BFR Pegawai : ', blockingFactor('pegawai'))
    print('Fan Out Rasio Pegawai : ', fanoutRasio('pegawai'))
    
    print('BFR Dirawat : ', blockingFactor('dirawat'))
    print('Fan Out Rasio Dirawat : ', fanoutRasio('dirawat'))
    
    print('BFR Fasilitas : ', blockingFactor('fasilitas'))
    print('Fan Out Rasio Fasilitas : ', fanoutRasio('fasilitas'))
    print('')

def menuDua():
    print('Menu 2 : Jumlah Blok')
    print('')
    
    print('Tabel Data Pegawai :', totalBlokData('pegawai'), ' blok')
    print('Indeks Pegawai :', totalBlokIndeks('pegawai'), ' blok')
    
    print('Tabel Data Pegawai :', totalBlokData('dirawat'), ' blok')
    print('Indeks Pegawai :', totalBlokIndeks('dirawat'), ' blok')
    
    print('Tabel Data Pegawai :', totalBlokData('fasilitas'), ' blok')
    print('Indeks Pegawai :', totalBlokIndeks('fasilitas'), ' blok')
    
def menuTiga():
    print('Menu 3 : Pencarian Rekord')
    print('')
    rekord = int(input("Cari Rekord ke : "))
    tabel = input("Nama Tabel : ")

    blok_i = jumBlokIndeks(rekord, tabel)

    print ('Menggunakan indeks, jumlah blok yang diakses : ', blok_i)
    print ('Tanpa indeks, jumlah blok yang diakses : ', jumBlokNonIndeks(rekord, tabel))


# In[37]:


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

	if sql['select'].__class__.__name__ == 'list':
		for col in sql['select']:
			if 'value' in col:
				cols.append(col['value'])
			else:
				cols.append(col)
	elif sql['select'].__class__.__name__ == 'str':
		cols = sql['select']
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
			return 'Unknown column '+ cols

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


# In[38]:


def menuEmpat():
    try:
        query = input("Masukkan query : ")
        parse_res = parse(query)
    except:
        print('Insufficient Memory.')
        
    if(checkSharedPool(query)):
        printSharedPool(query)
    else:    
        if (parse_res['conditions'] == ""):
            qep = joinAr(query)
        elif (parse_res['join'] == ""):
            qep = whereAr(query)
        saveSharedPool(query, parse)
        print(qep)
    
def menuLima():
    data = readFile('shared-pool.json')
    print(data)
    
menu = int(input("Menu : "))
try:
    while (menu != 0):
        if (menu == 1):
            menuSatu()
        elif (menu == 2):
            menuDua()
        elif (menu == 3):
            menuTiga()
        elif (menu == 4):
            menuEmpat()
        elif (menu == 5):
            menuLima()
        menu = int(input("Menu : "))
except:
    print('Something is going wrong :/')

