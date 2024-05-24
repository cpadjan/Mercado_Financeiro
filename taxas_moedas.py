'''
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#                      Baixa os dados do SGS-Bacen                            #
#                                                                             #
# 1) Baixa o as do SGS-Bacen                                                  #
# 2) Acumula as taxas Selic e CDI                                             #
# 3) Salva os dados em arquivos TXT                                           #
# 4) Arquivo em Excel serve como front para exibir e analisar os dados        #
#                                                                             #
# Versão: 2024-05-24                                                          #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
'''


import sgs
import numpy as np
from datetime import date
import concurrent.futures
import os
import time

inicio = time.perf_counter()

# datas

data_inicial_cdi = '02/01/1998'
data_inicial_selic = '04/01/2021'
data_final = date.today().strftime('%d/%m/%Y')


# CAPTURA DOS DADOS E CRIAÇÃO DOS DATAFRAMES #

def taxa_CDI(*knargs):
    dt1 = data_inicial_cdi
    dt2 = data_final
    taxa_CDI = sgs.dataframe([4389], start=dt1, end=dt2)
    taxa_CDI.rename(columns={4389: 'CDI_taxa'}, inplace=True)
    taxa_CDI.dropna(inplace=True)
    taxa_CDI['CDI_fator'] = round( (1 + taxa_CDI['CDI_taxa'] / 100) ** (1 / 252), 9 )
    taxa_CDI['CDI_acum'] = round ( np.cumprod(taxa_CDI['CDI_fator']), 9 )
    taxa_CDI['Data'] = taxa_CDI.index
    total_registro_cdi = len(taxa_CDI)
    index_cdi = np.array(range(total_registro_cdi))
    taxa_CDI.set_index(index_cdi, inplace=True)
    taxa_CDI = taxa_CDI[['Data', 'CDI_taxa', 'CDI_fator', 'CDI_acum']]

    return taxa_CDI


def taxa_Selic(*knargs):
    dt1 = data_inicial_selic
    dt2 = data_final
    taxa_Selic = sgs.dataframe([1178], start=dt1, end=dt2)
    taxa_Selic.rename(columns={1178: 'Selic_taxa'}, inplace=True)
    taxa_Selic.dropna(inplace=True)
    taxa_Selic['Selic_fator'] = round ( (1 + taxa_Selic['Selic_taxa'] / 100) ** (1 / 252), 9 )
    taxa_Selic['Selic_acum'] = round ( np.cumprod(taxa_Selic['Selic_fator']), 9 )
    taxa_Selic['Data'] = taxa_Selic.index
    total_registro_selic = len(taxa_Selic)
    index_selic = np.array(range(total_registro_selic))
    taxa_Selic.set_index(index_selic, inplace=True)
    taxa_Selic = taxa_Selic[['Data', 'Selic_taxa', 'Selic_fator', 'Selic_acum']]

    return taxa_Selic


def moedas_bacen(*knargs):
    dt1 = data_inicial_cdi
    dt2 = data_final
    consulta_moeda = sgs.dataframe([1, 10813, 21619, 21620], start=dt1, end=dt2)
    consulta_moeda.rename(columns={1: 'USD_venda', 10813: 'USD_compra', 21619: 'EUR_venda', 21620: 'EUR_compra'},
                          inplace=True)
    consulta_moeda.fillna(0, inplace=True)
    consulta_moeda['Data'] = consulta_moeda.index
    total_registro_moeda = len(consulta_moeda)
    moeda_index = np.array(range(total_registro_moeda))
    consulta_moeda.set_index(moeda_index, inplace=True)
    consulta_moeda = consulta_moeda[['Data', 'USD_venda', 'USD_compra', 'EUR_venda', 'EUR_compra']]

    return consulta_moeda

def indicadores(*knargs):
    dt1 = data_inicial_cdi
    dt2 = data_final
    consulta_indicador = sgs.dataframe([433, 189, 4382, 188, 24364, 20714, 21082], start=dt1, end=dt2)
    consulta_indicador.rename(columns={433: 'IPCA', 189: 'IGPM', 4382: 'PIB_acum_12m', 188: 'INPC', 24364: 'IBC-Br',
                                       20714: 'Tx_media_ops_credito', 21082: 'Inadimplencia'}, inplace=True)
    consulta_indicador['Data'] = consulta_indicador.index
    total_registro_ind = len(consulta_indicador)
    consulta_index = np.array(range(total_registro_ind))
    consulta_indicador.set_index(consulta_index, inplace=True)
    consulta_indicador.fillna(0, inplace=True)

    consulta_indicador = consulta_indicador[
        ['Data', 'IPCA', 'IGPM', 'PIB_acum_12m', 'INPC', 'IBC-Br', 'Tx_media_ops_credito', 'Inadimplencia']]

    return consulta_indicador

def ind_tr(*knargs):
    dt1 = data_inicial_cdi
    dt2 = data_final
    ind_tr = sgs.dataframe([226], start=dt1, end=dt2)
    ind_tr.rename(columns={226 : 'TR' }, inplace=True)
    ind_tr['Data'] = ind_tr.index

    total_registro = len(ind_tr)
    ind_index = np.array(range(total_registro))
    ind_tr.set_index(ind_index, inplace=True)
    ind_tr.fillna(0, inplace=True)

    ind_tr = ind_tr[['Data', 'TR']]


    return ind_tr

def ind_endiv(*knargs):
    dt1 = data_inicial_cdi
    dt2 = data_final
    ind_endiv = sgs.dataframe([29037], start=dt1, end=dt2)
    ind_endiv.rename(columns={29037 : 'Endividamento' }, inplace=True)
    ind_endiv['Data'] = ind_endiv.index

    total_registro = len(ind_endiv)
    ind_index = np.array(range(total_registro))
    ind_endiv.set_index(ind_index, inplace=True)
    ind_endiv.fillna(0, inplace=True)

    ind_endiv = ind_endiv[['Data', 'Endividamento']]


    return ind_endiv


if __name__ == '__main__':
    # Ajuste o número de workers para corresponder ao número de núcleos de CPU
    max_workers = os.cpu_count()

    # Executa as functions usando o multi-processamento
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        cod1 = executor.submit(taxa_CDI, data_inicial_cdi, data_final)
        cod2 = executor.submit(taxa_Selic, data_inicial_selic, data_final)
        cod3 = executor.submit(moedas_bacen, data_inicial_cdi, data_final)
        cod4 = executor.submit(indicadores, data_inicial_cdi, data_final)
        cod5 = executor.submit(ind_tr, data_inicial_cdi, data_final)
        cod6 = executor.submit(ind_endiv, data_inicial_cdi, data_final)

        df_cdi = cod1.result()
        df_selic = cod2.result()
        df_moedas = cod3.result()
        df_ind = cod4.result()
        df_ind_tr = cod5.result()
        df_ind_endiv = cod6.result()


        # Gera os arquivo TXT
        df_cdi.sort_values(by=['Data'], ascending=[True]).to_csv('txt\CDI.txt', sep=';', index=False, decimal = ',', date_format = '%d/%m/%Y')
        df_selic.sort_values(by=['Data'], ascending=[True]).to_csv('txt\Selic.txt', sep=';', index=False,  decimal = ',')
        df_moedas.sort_values(by=['Data'], ascending=[True]).to_csv('txt\Moedas.txt', sep=';', index=False,  decimal = ',')
        df_ind.sort_values(by=['Data'], ascending=[True]).to_csv('txt\Ind.txt', sep=';', index=False,  decimal = ',')
        df_ind_tr.sort_values(by=['Data'], ascending=[True]).to_csv('txt\Ind_TR.txt', sep=';', index=False, decimal=',')
        df_ind_endiv.sort_values(by=['Data'], ascending=[True]).to_csv('txt\Ind_Endiv.txt', sep=';', index=False, decimal=',')

        print('Processamento concluido.')

        fim = time.perf_counter()

        # Calcula o tempo total decorrido
        tempo_total = fim - inicio

        print(f"Tempo de execução: {round(tempo_total, 3)} segundos")

