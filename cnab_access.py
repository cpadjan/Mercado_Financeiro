from os import listdir, rename, remove
from os.path import isfile, join
import pyodbc

# PASTA DOS ARQUIVOS
path = r'...\Py\cnab\txt'
path_lido = r'...\Py\cnab\txt\lido'

# VARIAVEIS GLOBAL
data_arq = ''
data_arq = ''

def data_hora_arquivo (linha):
    data_arq = linha[143:145] + '/' + linha[145:147] + '/' + linha[147:151]
    hora_arq = linha[151:153] + ':' + linha[153:155] + ':' + linha[155:157]
    return data_arq, hora_arq


def Layout_Itau_Linha_3 (linha):
    qdt_resgistro_itau = 0


    for linha in cnab:
        cod_linha = linha[7:8]

        if cod_linha == '0':
            data_arq = data_hora_arquivo(linha)[0]
            hora_arq = data_hora_arquivo(linha)[1]


        if cod_linha == '3':

            data_lancamento = linha[142:144] + '/' + linha[144:146] + '/' + linha[146:150]
            c_c = linha[59:70].strip() + '/' + linha[71:72].strip()
            historico1 = linha[176:201].strip()
            historico2 = linha[208:241].strip()
            valor_txt = linha[150:168].strip()
            cod_lancamento = linha[169:172].strip()
            tipo = linha[168:169].strip()
            cod_fluxo = linha[172:176].strip()

            qdt_resgistro_itau += 1

            #print(f"{data_lancamento} | {c_c} | {historico1} | {historico2} | {valor_txt} | {cod_lancamento} | {tipo} | {cod_fluxo} | {data_arq} | {hora_arq}  ")

            conn = pyodbc.connect(
                #r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=...\Py\cnab\bd\teste1.accdb;')
                #r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=...\Py\cnab\bd\2022_3_itau.accdb;')
                r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=...\Py\cnab\bd\2023_3_itau.accdb;')
            cursor = conn.cursor()
            sql = f"Insert into linha_3 (data_lancamento, c_c, historico1, historico2, valor_txt,  cod_lancamento, tipo, cod_fluxo, data_arq, hora_arq) VALUES ( '{data_lancamento}', '{c_c}', '{historico1}', '{historico2}', '{valor_txt}', '{cod_lancamento}', '{tipo}', '{cod_fluxo}' , '{data_arq}', '{hora_arq}'  ) ;"
            cursor.execute(sql)
            cursor.commit()
            cursor.close()


    print(f'Itau: {qdt_resgistro_itau}')
    print('*****')

def Layout_Bradesco_Linha_3 (linha):
    qdt_resgistro_bradesco = 0

    for linha in cnab:
        cod_linha = linha[7:8]

        if cod_linha == '0':
            data_arq = data_hora_arquivo(linha)[0]
            hora_arq = data_hora_arquivo(linha)[1]

        if cod_linha == '3':

            data_lancamento = linha[142:144] + '/' + linha[144:146] + '/' + linha[146:150]
            c_c = linha[57:70].strip() + '/' + linha[70:71].strip()
            historico1 = linha[176:201].strip()
            historico2 = linha[208:241].strip()
            nr_documento =linha[201:208].strip()
            valor_txt = linha[150:168].strip()
            cod_lancamento = linha[169:172].strip()
            tipo = linha[168:169]

            qdt_resgistro_bradesco += 1

            conn = pyodbc.connect(
                #r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=...\Py\cnab\bd\2022_3_bradesco.accdb;')
                r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=...\Py\cnab\bd\2023_3_bradesco.accdb;')
            cursor = conn.cursor()
            sql = f"Insert into linha_3 (data_lancamento, c_c, historico1, historico2, nr_documento, valor_txt,  cod_lancamento, tipo, data_arq, hora_arq) VALUES ( '{data_lancamento}', '{c_c}', '{historico1}', '{historico2}','{nr_documento}' , '{valor_txt}', '{cod_lancamento}', '{tipo}', '{data_arq}', '{hora_arq}'  ) ;"
            cursor.execute(sql)
            cursor.commit()

            cursor.close()

    print(f'Bradesco: {qdt_resgistro_bradesco}')
    print('*****')

def Layout_Santander_Linha_3 (linha):
    qdt_resgistro_santander = 0
    for linha in cnab:
        cod_linha = linha[7:8]

        if cod_linha == '0':
            data_arq = data_hora_arquivo(linha)[0]
            hora_arq = data_hora_arquivo(linha)[1]

        if cod_linha == '3':

            data_lancamento = linha[142:144] + '/' + linha[144:146] + '/' + linha[146:150]
            c_c = linha[57:70].strip() + '/' + linha[70:71].strip()
            historico1 = linha[176:201].strip()
            historico2 = linha[207:232].strip()
            nr_documento =linha[201:207].strip()
            valor_txt = linha[150:168].strip()
            cod_lancamento = linha[169:172].strip()
            tipo = linha[168:169].strip()

            qdt_resgistro_santander += 1

            conn = pyodbc.connect(
                #r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=...\Py\cnab\bd\2022_3_santander.accdb;')
                r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=...\Py\cnab\bd\2023_3_santander.accdb;')
            cursor = conn.cursor()
            sql = f"Insert into linha_3 (data_lancamento, c_c, historico1, historico2, nr_documento, valor_txt,  cod_lancamento, tipo, data_arq, hora_arq) VALUES ( '{data_lancamento}', '{c_c}', '{historico1}', '{historico2}','{nr_documento}' , '{valor_txt}', '{cod_lancamento}', '{tipo}', '{data_arq}', '{hora_arq}'  ) ;"
            cursor.execute(sql)
            cursor.commit()
            cursor.close()

    print(f'Santander: {qdt_resgistro_santander}')
    print('*****')

def Layout_BV_Linha_3 (linha):
    qdt_resgistro_bv = 0
    for linha in cnab:
        cod_linha = linha[7:8]

        if cod_linha == '0':
            data_arq = data_hora_arquivo(linha)[0]
            hora_arq = data_hora_arquivo(linha)[1]

        if cod_linha == '3':

            data_lancamento = linha[142:144] + '/' + linha[144:146] + '/' + linha[146:150]
            c_c = linha[58:70].strip() + '/' + linha[70:71].strip()
            historico1 = linha[176:201].strip()
            historico2 = linha[208:241].strip()
            nr_documento =linha[201:207].strip()
            valor_txt = linha[150:168].strip()
            cod_lancamento = linha[169:172].strip()
            tipo = linha[168:169].strip()
            protocolo = linha[201:212].strip()

            qdt_resgistro_bv += 1

            print(f"{data_lancamento} | {c_c} | {historico1} | {historico2} | {nr_documento} | {valor_txt} | {cod_lancamento} | {tipo} | {protocolo} | {data_arq} | {hora_arq}")

            conn = pyodbc.connect(
                r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=...\Py\cnab\bd\2023_3_bv.accdb;')
            cursor = conn.cursor()
            sql = f"Insert into linha_3 (data_lancamento, c_c, historico1, historico2, nr_documento, valor_txt,  cod_lancamento, tipo, protocolo, data_arq, hora_arq) VALUES ( '{data_lancamento}', '{c_c}', '{historico1}', '{historico2}','{nr_documento}' , '{valor_txt}', '{cod_lancamento}', '{tipo}','{protocolo}', '{data_arq}', '{hora_arq}'  ) ;"
            cursor.execute(sql)
            cursor.commit()
            cursor.close()

    print(f'Banco BV: {qdt_resgistro_bv}')
    print('*****')

# LE OS ARQUIVOS QUE ESTÃO NA PASTA E CRIA UMA LISTA
files = [f for f in listdir(path) if isfile(join(path, f))]
qtde_arquivos = len(files)

# PASSA POR TODOS OS ARQUIVOS QUE ESTAO NA LISTA
for arq in files:
    cnab = open(path + "/" + arq , 'r')
    banco = cnab.readline(3)
    cnab.close()


    if banco == '341':
        cnab = open(path + "/" + arq, 'r')

        for linha in banco:
            Layout_Itau_Linha_3(linha)

        cnab.close()

        #Move o arquivo para uma pasta
        try:
            rename(path + "/" + arq, path_lido + "/" + arq)
        except FileExistsError:
            remove(path_lido + "/" + arq)
            rename(path + "/" + arq, path_lido + "/" + arq)


    if banco == '237':
        cnab = open(path + "/" + arq, 'r')

        for linha in banco:
            Layout_Bradesco_Linha_3(linha)
        cnab.close()

        # Move o arquivo para uma pasta
        try:
            rename(path + "/" + arq, path_lido + "/" + arq)
        except FileExistsError:
            remove(path_lido + "/" + arq)
            rename(path + "/" + arq, path_lido + "/" + arq)

    if banco == '033':
        cnab = open(path + "/" + arq, 'r')

        for linha in banco:
            Layout_Santander_Linha_3(linha)
        cnab.close()

        # Move o arquivo para uma pasta
        try:
            rename(path + "/" + arq, path_lido + "/" + arq)
        except FileExistsError:
            remove(path_lido + "/" + arq)
            rename(path + "/" + arq, path_lido + "/" + arq)

    if banco == '655':
        cnab = open(path + "/" + arq, 'r')

        for linha in banco:
            Layout_BV_Linha_3(linha)
        cnab.close()

        # Move o arquivo para uma pasta
        try:
            rename(path + "/" + arq, path_lido + "/" + arq)
        except FileExistsError:
            remove(path_lido + "/" + arq)
            rename(path + "/" + arq, path_lido + "/" + arq)

