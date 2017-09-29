from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib as plt
from scipy import stats
import seaborn as sns
import pyodbc
import os
import shutil

# Exemplo de plot com seaborn
#iris = sns.load_dataset("iris");
#g1 = sns.distplot(iris.sepal_length, rug = True, fit = stats.gausshyper);




defaultPath = 'D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/'
defaultPathProcessFile = 'D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/processado'

def extractAdquirentesFiles() :
    fileName = 'transacoes-venda20170601-20170615.csv'
    df = pd.read_csv(defaultPath + 'dados/' + fileName, delimiter=';', dtype=object, encoding='iso8859_2')
    df = df[df['ID Único EQUALS'].isnull() == False]

    df['Parcela'] = df['Parcela'].fillna(0).astype(int)
    df['Valor Bruto'] = df['Valor Bruto'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Comissăo'] = df['Valor Comissăo'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Líquido'] = df['Valor Líquido'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Rejeitado'] = df['Valor Rejeitado'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    #df = df.iloc[0:len(df), 2:38];
    return df

def extractAdquirenteFile(fileName) :
    df = pd.read_csv(fileName, delimiter=';', dtype=object, encoding='iso8859_2')
    df = df[df['ID Único EQUALS'].isnull() == False]

    df['Parcela'] = df['Parcela'].fillna(0).astype(int)
    df['Valor Bruto'] = df['Valor Bruto'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Comissăo'] = df['Valor Comissăo'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Líquido'] = df['Valor Líquido'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Rejeitado'] = df['Valor Rejeitado'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    return df

# Deprecated
#def SliceAdquirentesFilesHeaderAndRows(df) :
#    df_header = df[df['ID Único EQUALS'].isnull() == True];
#    df_header = df_header[['Nro. Autorização', 'Vlr. Bruto', 'Vlr. Comissão', 'Vlr. Líquido', 'Vlr. Rejeitado']];
#    df_rows = df[df['ID Único EQUALS'].isnull() == False];
#    df_rows = df_rows.iloc[0:len(df_rows), 2:38];
    #df_rows = df_rows.replace(np.nan, 'NULL') somente para inserir no SQL SERVER

#    return df_header, df_rows;

def extractIRFiles():
    fileName = "transacoes-pagamento20170601-20170605.csv"
    df = pd.read_csv(defaultPath + 'dados/' + fileName, delimiter=';', dtype=object, encoding='iso8859_2')
    # Munging
    df['Parcela'] = df['Parcela'].fillna(0).astype(int)
    df['Valor do Pagamento'] = df['Valor do Pagamento'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Líquido'] = df['Valor Líquido'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor da Parcela'] = df['Valor da Parcela'].fillna('0')
    df['Valor Adicional 1'] = df['Valor Adicional 1'].fillna('0')
    df['Valor Adicional 2'] = df['Valor Adicional 2'].fillna('0')
    df['Valor da Diferença'] = df['Valor da Diferença'].fillna('0')
    df['Valor da Parcela'] = df['Valor da Parcela'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Adicional 1'] = df['Valor Adicional 1'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Adicional 2'] = df['Valor Adicional 2'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor da Diferença'] = df['Valor da Diferença'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    # Slice
    df = df[df['Bandeira'].isnull() == False]
    #df_head = df[df['Bandeira'].isnull() == True]
    #df_rows = df_rows.iloc[0:len(df_rows), 2:40]
    df = df.iloc[0:len(df), 0:33]

    return df

def extractIRFile(fileName) :
    df = pd.read_csv(fileName, delimiter=';', dtype=object, encoding='latin_1')
    #Slice only transaction rows
    df = df[df['Bandeira'].isnull() == False]
    df['Parcela'] = df['Parcela'].fillna(0).astype(int)
    df['Valor do Pagamento'] = df['Valor do Pagamento'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Líquido'] = df['Valor Líquido'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor da Parcela'] = df['Valor da Parcela'].fillna('0')
    df['Valor Adicional 1'] = df['Valor Adicional 1'].fillna('0')
    df['Valor Adicional 2'] = df['Valor Adicional 2'].fillna('0')
    df['Valor da Diferença'] = df['Valor da Diferença'].fillna('0')
    df['Valor da Parcela'] = df['Valor da Parcela'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Adicional 1'] = df['Valor Adicional 1'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Adicional 2'] = df['Valor Adicional 2'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor da Diferença'] = df['Valor da Diferença'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)

    df = df.iloc[0:len(df), 0:33]
    return df

def extractCancelFiles():
    fileName = 'vendas-canceladas-contestadas20170601-20170615.csv'
    df = pd.read_csv(defaultPath + 'dados/' + fileName, delimiter=';', dtype=object, encoding='latin_1')
    df_rows = df[df['Bandeira'].isnull() == False]
    df_rows = df_rows.iloc[0:len(df_rows), 0:33]
    # Munging
    df_rows['Parcela'] = df_rows['Parcela'].fillna(0).astype(int)
    df_rows['Dt. Venda'] = df_rows['Dt. Venda'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df_rows['Data Captura'] = df_rows['Data Captura'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df_rows['Data do Cancelamento'] = df_rows['Data do Cancelamento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df_rows['Valor Bruto'] = df_rows['Valor Bruto'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df_rows['Valor Comissão'] = df_rows['Valor Comissão'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df_rows['Valor Líquido'] = df_rows['Valor Líquido'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df_rows['Vlr. Bruto Cancelamento'] = df_rows['Vlr. Bruto Cancelamento'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df_rows['Vlr. Líquido Cancelamento'] = df_rows['Vlr. Líquido Cancelamento'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)

    return df_rows

def getFileNameGroup():
    pasta = 'D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/'
    adquirenteFiles = []
    irFiles = []
    cancelamentoFiles = []

    caminhos = [os.path.join(pasta, nome) for nome in os.listdir(pasta)]
    arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
    csvFileNames = [arq for arq in arquivos if arq.lower().endswith(".csv")]
    csvFileNames = pd.DataFrame(csvFileNames)
    csvFileNames.columns = ['File']

    for fileName in csvFileNames['File']:
        if 'transacoes-venda' in fileName:
            adquirenteFiles.append(fileName)
        elif 'transacoes-pagamento' in fileName:
            irFiles.append(fileName)
        elif 'vendas-canceladas-contestadas' in fileName:
            cancelamentoFiles.append(fileName)

    return adquirenteFiles, irFiles, cancelamentoFiles

def moveFile(fileName):
    return shutil.move(fileName, defaultPathProcessFile)


#print(extractCancelFiles().head())

####################################################################################################
## TESTE

#print(extractIRFiles().head(10))

#df = extractAdquirentesFiles()
#print(df.head(10))



############
pasta = 'D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/'
caminhos = [os.path.join(pasta, nome) for nome in os.listdir(pasta)]
arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
csvFileNames = [arq for arq in arquivos if arq.lower().endswith(".csv")]

#print(csvFileNames)
csvFileNames = pd.DataFrame(csvFileNames)
#print(csvFileNames.count())
#print(csvFileNames.head())
csvFileNames.columns = ['File']


#adquirenteFiles = []
#irFiles = []
#cancelamentoFiles = []

#x = []

#for fileName in csvFileNames['File']:
#    if 'transacoes-venda' in fileName :
#        adquirenteFiles.append(fileName)
#    elif 'transacoes-pagamento' in fileName :
#        irFiles.append(fileName)
#    elif 'vendas-canceladas-contestadas' in fileName:
#        cancelamentoFiles.append(fileName)

#for fileName in adquirenteFiles:
#    x = extractAdquirenteFile(fileName)
#    print(x.head())
####################################################

#print(adquirenteFiles)
#print(irFiles)
#print(cancelamentoFiles)


#arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
#jpgs = [arq for arq in arquivos if arq.lower().endswith(".jpg")]

#print(ExtractCanceledFiles().head(10))



#--------------------------------------------------------------------------------------------------------
# FUNCTIONS DE SQL
#--------------------------------------------------------------------------------------------------------

#df_head, df_rows = extractIRFiles();
#print(df_head.head(10))
#print(df_rows.head(10))


#------------------------------------------------------------------------------------
#          RASCUNHO

#df = importPayments();
#print(df.head(10)[['ID Único ERP', 'ID Único EQUALS', 'Data']]);

#df_header = df.copy();
#df_header[df_header['ID Único EQUALS'].isnull()==True].head();

#df = extractAdquirentesFiles();
#dfHeader, dfRows = SliceAdquirentesFilesHeaderAndRows(df);
#conn = openConn();
#cursor = openCursor(conn);
#insertDsTransacaoAdquirente(cursor, conn, dfRows)

#df_rows = df[df['ID Único EQUALS'].isnull() == False];
#df_rows = pd.DataFrame(df_rows.iloc[0:len(df_rows), 2:38]);
#df_rows = df_rows.replace(np.nan, 'NULL');

#print(df_rows.head(5))

#conn = openConn();
#cursor = openCursor(conn);
#cursor.executemany(
#        "INSERT INTO [dbo].[dsTransacaoAdquirente] ([NroAutorizacao] ,[IDERP] ,[IDEQUALS] ,[Adquirente] ,[Tipo] ,[Produto] ,[Estabelecimento] ,[CategoriaEstabelecimento] ,[Bandeira] ,[LoteRO] ,[NroTerminal] ,[MeioCaptura] ,[Data] ,[DtCaptura] ,[Hora] ,[NroCartao] ,[OrigemCartao] ,[NSU] ,[TID] ,[Parc] ,[LoteUnico] ,[NroReferencia] ,[NroFiliacao] ,[VlrBruto] ,[VlrComissao] ,[Taxa] ,[TxContrato] ,[VlrLi­quido] ,[VlrRejeitado] ,[Situacao] ,[DtSituacao] ,[Conciliada] ,[MJustificativa] ,[NomeTarefa] ,[SituacaoTarefa] ,[ConciliacaoParam]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
#    df_rows.values.tolist());
#conn.commit();



#dfHeader, dfRows = SliceAdquirentesFilesHeaderAndRows(df);
#conn = openConn();
#cursor = openCursor(conn);


#print(pd.DataFrame(dfRows).head(5));

#insertDsTransacaoAdquirente(cursor,conn,dfRows);

#insertDsTransacaoAdquirente(conn, cursor, dfRows)

#cursor.executemany("insert into dsEstabelecimentoAdquirente (Estabelecimento, VlrBruto, VlrComissao, VlrLiquido, VlrRejeitado) values (?,?,?,?,?)", dfHeader.values.tolist());
#conn.commit();

#map(insertDsEstabelecimento_tmp, dfHeader['Nro. Autorização'], dfHeader['Vlr. Bruto'], dfHeader['Vlr. Comissão'], dfHeader['Vlr. Líquido'], dfHeader['Vlr. Rejeitado'])