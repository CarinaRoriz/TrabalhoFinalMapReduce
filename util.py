import unicodedata

def removePontuacao(palavra):
    caracteres = '''!()[]{};:'",<>.?\@#$%^&*~`'''
    resultado = ""
    for char in palavra:
        if char not in caracteres:
            resultado = resultado + char
    return resultado
    
def correcaoAcentos(palavra):
    try:
        palavra = unicode(palavra, 'utf-8')
    except NameError:
        pass
    palavra = unicodedata.normalize('NFD', palavra).encode('ascii', 'ignore').decode("utf-8")
    return str(palavra)

