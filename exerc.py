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
    from util import correcaoAcentos;
    from util import removePontuacao;
    
    for line in v.splitlines():
        for item in line.split(':::')[1].split('::'):
            autorFomatado = removePontuacao(item)
            autorFomatado = correcaoAcentos(autorFomatado)
            yield autorFomatado, line.split(':::')[2].split(' ')

def reducefn(k, v):
    print 'reduce ' + k
    listaPalavras = {}
    from stopwords import allStopWords
    from util import correcaoAcentos;
    from util import removePontuacao;

    for index, item in enumerate(v):
        for palavra in item:
            palavraFormata = removePontuacao(palavra)
            palavraFormata = correcaoAcentos(palavraFormata)
            palavraFormata = palavraFormata.lower()
            if(palavraFormata not in allStopWords):
                if palavraFormata in listaPalavras:
                    listaPalavras[palavraFormata] = int(listaPalavras.get(palavraFormata)) + 1
                else:
                    listaPalavras[palavraFormata] = 1
    
    return listaPalavras

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password='changeme')

w = csv.writer(open('D:\\Pos graduacao\\Big Data\\TrabalhoFinal\\result.csv', 'w'))
for k, v in results.items():
    w.writerow([k, v])

#Buscar as palavras mais usadas pelos autores Grzegorz Rozenberg e Philip S. Yu.
resultado = {}
file1 = open("resultadoExerc6.txt","w") 
for k, v in results.items():
    if(k in {'Grzegorz Rozenberg', 'Philip S Yu'}):
        maiorQuant = 0
        segundoMaiorQuant = 0
        file1.write('\n' + k)
        
        for a,b in v.items():
            if b > maiorQuant:
               segundoMaiorQuant = maiorQuant
               maiorQuant = b
            elif b > segundoMaiorQuant and b != maiorQuant:
               segundoMaiorQuant = b

        sort_orders = sorted(v.items(), key=lambda x: x[1], reverse=True)
        for d in sort_orders:
            if d[1] == maiorQuant:
                file1.write('\n' + d[0] + ' - ' + str(d[1])) 
            elif d[1] == segundoMaiorQuant:
                file1.write('\n' + d[0] + ' - ' + str(d[1])) 
        file1.write('\n=========================================')
file1.close() 