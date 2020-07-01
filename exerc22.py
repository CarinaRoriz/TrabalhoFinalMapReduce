import mincemeat;
import glob;
import csv;

text_files = glob.glob('D:\\Pos graduacao\\Big Data\\TrabalhoFinal\\Trab2.3\\*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name, file_contents(file_name)) for file_name in text_files)

def mapfn(k, v):
    print 'map ' + k
    for line in v.splitlines():
        for item in line.split(':::')[1].split('::'):
            yield item, line.split(':::')[2].split(' ')

def reducefn(k, v):
    print 'reduce ' + k
    total = 0
    listaAutores = {}

    for index, item in enumerate(v):
        listaPalavras = {}
        for palavra in item:
            print 'palavra -------------------------->' + palavra
            if palavra in listaPalavras:
                listaPalavras[palavra] = listaPalavras[palavra] + 1
            else:
                listaPalavras[palavra] = 1
        listaAutores[k] = listaPalavras 
    
    return listaAutores

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password='changeme')

w = csv.writer(open('D:\\Pos graduacao\\Big Data\\TrabalhoFinal\\result_map.csv', 'w'))
for k, v in results.items():
    w.writerow([k, v])