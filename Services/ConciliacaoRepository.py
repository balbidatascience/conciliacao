import pyodbc
import  numpy as np
import pandas as pd
from datetime import datetime

def openConn():
    return pyodbc.connect('DRIVER={SQL Server};SERVER=.;DATABASE=IR;UID=sa;PWD=@gabriel');

def openCursor(conn):
    return conn.cursor();

# Deprecated
#def insertDsEstabelecimento(cursor, conn, dfHeader):
#    cursor.executemany(
#        "insert into dsEstabelecimentoAdquirente (Estabelecimento, VlrBruto, VlrComissao, VlrLiquido, VlrRejeitado) values (?,?,?,?,?)",
#        dfHeader.values.tolist());
#    conn.commit();
#    return True;

def insertDsTransacaoAdquirente(cursor, conn, dfRows):
    # Tranformando os valores NAN/NA por NONE (que é aceito como NULL no SQL SERVER)
    dfRows = dfRows.astype(object).where(pd.notnull(dfRows), None);
    #format datetime para mm/dd/yyyy
    dfRows['Data'] = dfRows['Data'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    dfRows['Data Captura'] = dfRows['Data Captura'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'),na_action='ignore')
    dfRows['Data da Situaçăo'] = dfRows['Data da Situaçăo'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'),na_action='ignore')
    #dfRows['Data'] = dfRows['Data'].map(lambda x:str(x).replace(" 00:00:00", ""))
    #dfRows['Data'] = dfRows['Data'].map(lambda x:datetime.strptime(str(x), "%Y-%d-%m").strftime('%Y-%m-%d'), na_action='ignore')

    cursor.executemany(
        "INSERT INTO [dbo].[dsTransacaoAdquirente] ([NroAutorizacao] ,[IDERP] "
        ",[IDEQUALS] ,[Adquirente] ,[Tipo] ,[Produto] ,[Estabelecimento] "
        ",[CategoriaEstabelecimento] ,[Bandeira] ,[LoteRO] ,[NroTerminal] ,[MeioCaptura] ,"
        "[Data] ,[DtCaptura] ,[Hora] ,[NroCartao] ,[OrigemCartao] ,[NSU] ,[TID] ,[Parc] ,"
        "[LoteUnico] ,[NroReferencia] ,[NroFiliacao] ,[VlrBruto] ,[VlrComissao] ,[Taxa] ,"
        "[TxContrato] ,[VlrLi­quido] ,[VlrRejeitado] ,[Situacao] ,[DtSituacao] ,[Conciliada] ,"
        "[MJustificativa] ,[NomeTarefa] ,[SituacaoTarefa] ,[ConciliacaoParam]) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        dfRows.values.tolist());
    conn.commit();
    return True;


def insertDsTransacaoIR(cursor, conn, dfRows):
    # convert na, nan e nat para NULL
    dfRows = dfRows.astype(object).where(pd.notnull(dfRows), None)

    dfRows['Data da Transaçăo'] = dfRows['Data da Transaçăo'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    dfRows['Data da Venda'] = dfRows['Data da Venda'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')

    query = ("INSERT INTO [dbo].[dsTransacaoIR] "
           "([NroAutorizacao] "
           ",[IDERP] "
           ",[Cliente] "
           ",[Estabelecimento] "
           ",[CategoriaEstabelecimento] "
           ",[Adquirente] "
           ",[EstabERP] "
           ",[CanalVenda] "
           ",[NroCanal] "
           ",[FormaPagto] "
           ",[DtTransacao] "
           ",[DtVenda] "
           ",[LoteInterno] "
           ",[LoteAdq] "
           ",[IDContabilPagamento] "
           ",[IDContabilVenda] "
           ",[Bandeira] "
           ",[NumCartaoCredito] "
           ",[NSU] "
           ",[TID] "
           ",[Localizador] "
           ",[Parc] " 
           ",[VlrPagamento] "
           ",[VlrLiquido] "
           ",[VlrParcela] "
           ",[VlrAdic1] "
           ",[VlrAdic2] "
           ",[VlrDiferenca] "
           ",[Resultado] "
           ",[Conciliacao] "
           ",[NomeTarefa] "
           ",[SituacaoTarefa] "
           ",[SituacaoParam]) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

    cursor.executemany(query, dfRows.values.tolist())
    conn.commit()

    return True

def InsertDsCancelamento(cursor, conn, dfRows):
    # convert na, nan e nat para NULL
    dfRows = dfRows.astype(object).where(pd.notnull(dfRows), None)

    query = ("INSERT INTO [dbo].[dsCancelamento] "
           "([TipoCancelamento] "
           ",[NroAutorizacao] "
           ",[IDERP] "
           ",[Estabelecimento] "
           ",[CategoriaEstabelecimento] "
           ",[Adquirente] "
           ",[Bandeira] "
           ",[Tipo] "
           ",[Produto] "
           ",[Localizador] "
           ",[NroTerminal] "
           ",[LoteRO] "
           ",[LoteUnico] "
           ",[NrSequencia] "
           ",[NrReferenciaPedido] "
           ",[DtCaptura] "
           ",[DtVenda] "
           ",[DtCancelamento] "
           ",[NroCartao] "
           ",[NSU] "
           ",[TID] "
           ",[Parc] "
           ",[VlrBruto] "
           ",[VlrComissao] "
           ",[VlrLiquido] "
           ",[VlrBrutoCancelamento] "
           ",[VlrLiquidoCancelamento] "
           ",[Conciliacao] "
           ",[IDERPCanal] "
           ",[NomeCanal] "
           ",[MotivoCancelamento] "
           ",[NomeTarefa] "
           ",[SituacaoTarefa]) "
     "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

    cursor.executemany(query, dfRows.values.tolist())
    conn.commit()

    return True

#conn = openConn();
#cursor = openCursor()

#map(SqlService.insertDsEstabelecimento_tmp, cursor, conn,
#    dfHeader['Nro. Autorização'], dfHeader['Vlr. Bruto'], dfHeader['Vlr. Comissão'], dfHeader['Vlr. Líquido'], dfHeader['Vlr. Rejeitado'])

    #return print(insertDsEstabelecimento(cursor, conn));
