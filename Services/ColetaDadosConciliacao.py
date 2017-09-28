from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib as plt
from scipy import stats
import seaborn as sns
import pyodbc

# Exemplo de plot com seaborn
#iris = sns.load_dataset("iris");
#g1 = sns.distplot(iris.sepal_length, rug = True, fit = stats.gausshyper);


#--------------------------------------------------------------------------------------------------------
# FUNCTIONS DE DATAMUGING
#--------------------------------------------------------------------------------------------------------

defaultPath = 'D:/Gabriel/Develop/IR/Python/ConciliationCollect/';

def extractAdquirentesFiles() :
    fileName = 'DemonstrativoVendas16042017-30042017.csv';
    df = pd.read_csv(defaultPath + 'dados/' + fileName, delimiter=';', dtype=object);
    df['Parc.'] = df['Parc.'].fillna(0).astype(int);
    df['Vlr. Bruto'] = df['Vlr. Bruto'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float);
    df['Vlr. Comissão'] = df['Vlr. Comissão'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float);
    df['Vlr. Líquido'] = df['Vlr. Líquido'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float);
    df['Vlr. Rejeitado'] = df['Vlr. Rejeitado'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float);

    return df;


def SliceAdquirentesFilesHeaderAndRows(df) :
    df_header = df[df['ID Único EQUALS'].isnull() == True];
    df_header = df_header[['Nro. Autorização', 'Vlr. Bruto', 'Vlr. Comissão', 'Vlr. Líquido', 'Vlr. Rejeitado']];
    df_rows = df[df['ID Único EQUALS'].isnull() == False];
    df_rows = df_rows.iloc[0:len(df_rows), 2:38];
    #df_rows = df_rows.replace(np.nan, 'NULL') somente para inserir no SQL SERVER

    return df_header, df_rows;


def extractIRFiles():
    fileName = "VendaErpPagamentos16042017-30042017.csv"
    df = pd.read_csv(defaultPath + 'dados/' + fileName, delimiter=';', dtype=object, encoding='UTF-8')
    # Munging
    df['Parc.'] = df['Parc.'].fillna(0).astype(int)
    df['Qtd. Pagamentos'] = df['Qtd. Pagamentos'].fillna(0).astype(int)
    df['Vlr. Pagamento'] = df['Vlr. Pagamento'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Vlr. Líquido'] = df['Vlr. Líquido'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Vlr. Parcela'] = df['Vlr. Parcela'].fillna('0')
    df['Vlr. Adic. 1'] = df['Vlr. Adic. 1'].fillna('0')
    df['Vlr. Adic. 2'] = df['Vlr. Adic. 2'].fillna('0')
    df['Vlr. Diferença'] = df['Vlr. Diferença'].fillna('0')
    df['Vlr. Parcela'] = df['Vlr. Parcela'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Vlr. Adic. 1'] = df['Vlr. Adic. 1'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Vlr. Adic. 2'] = df['Vlr. Adic. 2'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Vlr. Diferença'] = df['Vlr. Diferença'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    # Slice
    df_rows = df[df['Bandeira'].isnull() == False]
    df_head = df[df['Bandeira'].isnull() == True]
    df_rows = df_rows.iloc[0:len(df_rows), 2:40]

    return df_head, df_rows


def ExtractCanceledFiles():
    fileName = 'ResumoVendasCanceladas16062017-30062017.csv'
    df = pd.read_csv(defaultPath + 'dados/' + fileName, delimiter=';', dtype=object, encoding='UTF-8')
    df_rows = df[df['Bandeira'].isnull() == False]
    df_rows = df_rows.iloc[0:len(df_rows), 2:36]
    # Munging
    df_rows['Parc.'] = df['Parc.'].fillna(0).astype(int)
    df_rows['Dt. Venda'] = df_rows['Dt. Venda'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df_rows['Dt. Captura'] = df_rows['Dt. Captura'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df_rows['Dt. Cancelamento'] = df_rows['Dt. Cancelamento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df_rows['Vlr. Bruto'] = df_rows['Vlr. Bruto'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df_rows['Vlr. Comissão'] = df_rows['Vlr. Comissão'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df_rows['Vlr. Líquido'] = df_rows['Vlr. Líquido'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df_rows['Vlr. Bruto Cancelamento'] = df_rows['Vlr. Bruto Cancelamento'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df_rows['Vlr. Líquido Cancelamento'] = df_rows['Vlr. Líquido Cancelamento'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)

    return df_rows





####################################################################################################
## TESTE

#df = extractAdquirentesFiles()
#print(df.head(10))

import os

pasta = 'D:/Gabriel/Develop/IR/Python/ConciliationCollect/dados/'
caminhos = [os.path.join(pasta, nome) for nome in os.listdir(pasta)]
arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
csvFileNames = [arq for arq in arquivos if arq.lower().endswith(".csv")]

print(csvFileNames)

csvFileNames = pd.DataFrame(csvFileNames)

print(csvFileNames.count())
print(csvFileNames)
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