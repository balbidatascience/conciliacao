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
    dfRows['Data da Situação'] = dfRows['Data da Situação'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'),na_action='ignore')

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

    dfRows['Data da Transação'] = dfRows['Data da Transação'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
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

def insertDsCancelamento(cursor, conn, dfRows):
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

def insertDsMovimentoFinanceiro(conn, cursor, df):
    # convert na, nan e nat para NULL
    df = df.astype(object).where(pd.notnull(df), None)
    query = ("INSERT INTO [dbo].[dsMovimentoFinanceiro] "
           "([Data] "
           ",[Adquirente] "
           ",[Estabelecimento] "
           ",[Categoria do Estabelecimento] "
           ",[Bandeira] "
           ",[Tipo da Venda] "
           ",[Produto] "
           ",[Banco] "
           ",[Agência] "
           ",[Conta] "
           ",[Lote] "
           ",[Lote Original] "
           ",[Lote Único] "
           ",[Histórico] "
           ",[Parcela] "
           ",[Valor Bruto] "
           ",[Valor Comissão] "
           ",[Valor Líquido Previsto] "
           ",[ValorLiquidoRealizado] "
           ",[Cessoes]) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

    cursor.executemany(query, df.values.tolist())
    conn.commit()
    return True

def saveCashFlow(conn, cursor, df):
    # convert na, nan e nat para NULL
    df = df.astype(object).where(pd.notnull(df), None)
    query = ("INSERT "
        "INTO[dbo].[dsFluxoCaixa] "       
        "([DataVencimento] "
        " , [Estabelecimento] "
        " , [CategoriaEstabelecimento] "
        " , [DataLoteVenda] "
        " , [MesAno] "
        " , [Adquirente] "
        " , [FiliacaoEstabelec] "
        " , [Bandeira] "
        " , [TipoVenda] "
        " , [Produto] "
        " , [Lote] "
        " , [LoteUnico] "
        " , [Parcela] "
        " , [QtdeParcelas] "
        " , [Banco] "
        " , [Agencia] "
        " , [Conta] "
        " , [ValorBruto] "
        " , [ValorComissao] "
        " , [VendasAntecCedidas] "
        " , [Cancelamentos] "
        " , [Chargeback] "
        " , [OutrosAjustes] "
        " , [ValorAntecipacoes] "
        " , [DescontosAntecCessoes] "
        " , [ValorPrevisto] "
        " , [ValorPago] "
        " , [Saldo] "
        " , [VendasCedidas] "
        " , [CessoesAvulsas] "
        " , [DescontosCessoesAvulsas]) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

    cursor.executemany(query, df.values.tolist())
    conn.commit()
    return True

def saveSaleIR(conn, cursor, df):
    # convert na, nan e nat para NULL
    df = df.astype(object).where(pd.notnull(df), None)
    query = ("INSERT INTO [dbo].[dsVendaAtivaLegado] "
           "([is_bileto] " 
           ",[gateway] " 
           ",[numero_venda_ir] " 
           ",[venda_bilheteria_id] " 
           ",[codigo_gateway] " 
           ",[nsu_host] " 
           ",[nsu_sitef] " 
           ",[paypal_id] " 
           ",[numero_autorizacao_adquirente] " 
           ",[data_compra] " 
           ",[quantidade_ingressos] " 
           ",[bandeira] " 
           ",[tipo_cartao] " 
           ",[nome_forma_pagamento] " 
           ",[valor_taxa_conveniencia_total] " 
           ",[valor_taxa_entrega_total] " 
           ",[valor_juros_total] " 
           ",[valor_ingressos_ativos_total] " 
           ",[valor_compra_original_total] " 
           ",[numero_parcelas] " 
           ",[numero_cartao] " 
           ",[nome_portador_cartao] " 
           ",[status_compra] " 
           ",[nome_comprador] " 
           ",[email_comprador] " 
           ",[facebook_id] " 
           ",[cpf_comprador] " 
           ",[telefone_comprador] " 
           ",[id_usuario] " 
           ",[ip_comprador] " 
           ",[plataforma_utilizada] " 
           ",[id_produtor_evento] " 
           ",[nome_produtor_evento] " 
           ",[nome_evento] " 
           ",[id_evento] " 
           ",[data_evento] " 
           ",[nome_local] " 
           ",[tipo_evento] " 
           ",[nota_fiscal_estabelecimento_sitef] " 
           ",[data_venda_completa] " 
           ",[nomes_precos_ingressos_unicos]) " 
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

    cursor.executemany(query, df.values.tolist())
    conn.commit()
    return True

def saveIRCancelLegacySale(conn, cursor, df):
        # convert na, nan e nat para NULL
        df = df.astype(object).where(pd.notnull(df), None)
        query = ("INSERT INTO [dbo].[dsVendaCanceladaLegado] "
           "([is_bileto] "
           ",[gateway] "
           ",[numero_venda_ir] "
           ",[venda_bilheteria_id] "
           ",[numero_cancelamento_ir] "
	       ",[cancelamento_bilheteria_id] "
           ",[codigo_gateway] "
           ",[nsu_host] "
           ",[nsu_sitef] "
           ",[paypal_id] "
           ",[numero_autorizacao_adquirente] "
           ",[data_compra] "
           ",[data_cancelamento] "
           ",[quantidade_ingressos] "
           ",[bandeira] "
           ",[tipo_cartao] "
           ",[nome_forma_pagamento] "
           ",[valor_taxa_conveniencia_total] "
           ",[valor_taxa_entrega_total] "
           ",[valor_juros_total] "
           ",[valor_ingressos_cancelados_total] "
           ",[valor_compra_cancelada_total] "
           ",[valor_compra_original_total] "
           ",[numero_parcelas] "
           ",[numero_cartao] "
           ",[nome_portador_cartao] "
           ",[status_compra] "
           ",[nome_comprador] "
           ",[email_comprador] "
           ",[facebook_id] "
           ",[cpf_comprador] "
           ",[telefone_comprador] "
           ",[id_usuario] "
           ",[ip_comprador] "
           ",[plataforma_utilizada] "
           ",[id_produtor_evento] "
           ",[nome_produtor_evento] "
           ",[nome_evento] "
           ",[id_evento] "
           ",[data_evento] "
           ",[nome_local] "
           ",[tipo_evento] "
           ",[nota_fiscal_estabelecimento_sitef] "
           ",[data_cancelamento_completa] "
           ",[nomes_precos_ingressos_unicos]) "
           " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

        cursor.executemany(query, df.values.tolist())
        conn.commit()
        return True



    #conn = openConn();
#cursor = openCursor()

#map(SqlService.insertDsEstabelecimento_tmp, cursor, conn,
#    dfHeader['Nro. Autorização'], dfHeader['Vlr. Bruto'], dfHeader['Vlr. Comissão'], dfHeader['Vlr. Líquido'], dfHeader['Vlr. Rejeitado'])

    #return print(insertDsEstabelecimento(cursor, conn));
