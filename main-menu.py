
# coding: utf-8

# In[8]:


import json
import numpy
import math
import moz_sql_parser


# In[9]:


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


# In[10]:


# read file
def readFile(name):
	with open(name) as json_file:
		return json.load(json_file)

data = readFile('data-dictionary.json')


# In[11]:


def fanoutRasio(tab):
    return math.floor(data['b'] / (data[tab]['v'] + data['p']))

def blockingFactor(tab):
    return math.floor(data['b'] / data[tab]['r'])

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


# In[12]:


def totalBlokData(tab):
    return math.ceil(data[tab]['n'] / blockingFactor(tab))

def totalBlokIndeks(tab):
    return math.ceil(data[tab]['n'] / fanoutRasio(tab))

def menuDua():
    print('Menu 2 : Jumlah Blok')
    print('')
    
    print('Tabel Data Pegawai :', totalBlokData('pegawai'), ' blok')
    print('Indeks Pegawai :', totalBlokIndeks('pegawai'), ' blok')
    
    print('Tabel Data Pegawai :', totalBlokData('dirawat'), ' blok')
    print('Indeks Pegawai :', totalBlokIndeks('dirawat'), ' blok')
    
    print('Tabel Data Pegawai :', totalBlokData('fasilitas'), ' blok')
    print('Indeks Pegawai :', totalBlokIndeks('fasilitas'), ' blok')
    print('')


# In[13]:


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
    
def menuTiga():
    print('Menu 3 : Pencarian Rekord')
    print('')
    rekord = int(input("Cari Rekord ke : "))
    tabel = input("Nama Tabel : ")

    blok_i = jumBlokIndeks(rekord, tabel)

    print ('Menggunakan indeks, jumlah blok yang diakses : ', blok_i)
    print ('Tanpa indeks, jumlah blok yang diakses : ', jumBlokNonIndeks(rekord, tabel))


# In[36]:


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
        
    print('Tabel : ', obj['tables'][0])
    print('Tabel : ', obj['tables'][1])
    print()
    print('List Kolom : ', obj['columns'][1])
        
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
            'condition' : '',
            'join' : '     ' + join[0]['condition']+ '-- BNLJ',
            'tables' : join[0]['tables'][0]+'    '+ join[1]['tables'][0],
            'cost' : join[0]['cost']
        }
    else:
        return {
            'projection' : projection['cols'][0] + ' --' + projection['algo'],
            'condition' : '',
            'join' : '     ' + join[1]['condition']+ '-- BNLJ',
            'tables' : join[1]['tables'][1]+'    '+ join[0]['tables'][1],
            'cost' : join[1]['cost']
        }


# In[32]:


#qep where
def whereAr(obj):
    data = readFile('data-dictionary.json')
    ars = []
    ars.append([])
    projection = {'cols' : obj['columns'], 'algo': 'on the fly'}
    where = []
    bfrs = {}
    blocks = {}
    
    if(data[obj['tables']]['pk'] in obj['conditions']):
        print('Tabel : ', obj['tables'])
        print('List Kolom : ', obj['columns'])
        
        where.append({
            'condition': obj['conditions'],
            'algo' : 'A1 Key',
            'tables': obj['tables'],
            'cost' : data[obj['tables']]['br'] / 2
        })
        where.append({
            'condition': obj['conditions'],
            'algo' : 'A2 Key',
            'tables': obj['tables'],
            'cost' : math.ceil(math.log(data[obj['tables']]['br'],fanoutRasio(obj['tables']))) + 1
        })
                       
        x = 1
        for w in where:
            print('QEP #'+str(x))
            print('PROJECTION', projection['cols'][0], '--', projection['algo'])
            print('SELECTION ',w['condition'], '--', w['algo'])
            print(w['tables'])
            print('Cost :', w['cost'])
            print()
            x += 1

        print('QEP Optimal:', min(where[0]['cost'], where[1]['cost']))
        if where[0]['cost'] < where[1]['cost']:
            return {
                'projection' : projection['cols'][0] + ' --' + projection['algo'],
                'condition' : where[0]['condition'],
                'join' : '',
                'tables' : where[0]['tables'][0]+'    '+ where[1]['tables'][0],
                'cost' : where[0]['cost']
            }
        else:
            return {
                'projection' : projection['cols'][0] + ' --' + projection['algo'],
                'condition' : where[1]['condition'],
                'join' : '',
                'tables' : where[1]['tables'][1]+'    '+ where[0]['tables'][1],
                'cost' : where[1]['cost']
            }
    else:
        algo = 'A1 Non Key'
        print('QEP Optimal')
        print('PROJECTION', projection['cols'][0], '--', projection['algo'])
        print('SELECTION ',obj['conditions'], '--', algo)
        print(obj['tables'])
        print('Cost :', data[obj['tables']]['br'])
        print()
        
        return {
            'projection' : projection['cols'][0] + ' --' + projection['algo'],
            'condition' : obj['conditions'] + ' ' + algo,
            'join' : '',
            'tables' : obj['tables'],
            'cost' : data[obj['tables']]['br']
        }


# In[24]:


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


# In[33]:


def saveSharedPool(query, parse):
    if 'join' in query:
        data_saved = joinAr(parse)
    else:
        data_saved = whereAr(parse)
        
    data_saved['query'] = query
    data = readFile('shared-pool.json')
    data.append(data_saved)
    
    with open('shared-pool.json', 'w') as outfile:  
        json.dump(data, outfile)
        
def checkSharedPool(query):
    data_r = readFile('shared-pool.json')
    return list(filter(lambda qry: qry['query'] == query, data_r))

def printSharedPool():
    data_r = readFile('shared-pool.json')
    print(data_r)
    
def menuEmpat():
   
    query = input("Masukkan query : ")
    parse_res = parse(query)
    
    if (parse_res['joins']==[] and parse_res['conditions']==''):
        print(query)
        print('cost = 0')
    else:
        if(checkSharedPool(query)):
            print(checkSharedPool(query))
        else:    
            saveSharedPool(query, parse_res)        
    
def menuLima():
    printSharedPool()
    
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

    
#select no_ktp, tgl_dirawat from pegawai join dirawat using no_ktp;
#select no_inventaris, tgl_dirawat from fasilitas join dirawat using no_inventaris;
#select no_ktp, tgl_dirawat from dirawat join pegawai using no_ktp;
#select no_inventaris, tgl_dirawat from dirawat join fasilitas using no_inventaris;
#select no_ktp from pegawai where no_ktp = 1;
#select gender from pegawai where gender = 1;

