from datetime import datetime
import pandas as pd
import os
import shutil

# Exemplo de plot com seaborn
#iris = sns.load_dataset("iris");
#g1 = sns.distplot(iris.sepal_length, rug = True, fit = stats.gausshyper);

defaultPath = 'D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/'
defaultPathProcessFile = 'D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/processado'

# Extrai os arquivos do menu Adquirentes/Vendas (lado adquirente/extratos eletrônicos)
def extractAdquirenteFile(fileName) :
    df = pd.read_csv(fileName, delimiter=';', dtype=object, encoding='latin_1')
    df = df[df['ID Único EQUALS'].isnull() == False]

    df['Parcela'] = df['Parcela'].fillna(0).astype(int)
    df['Valor Bruto'] = df['Valor Bruto'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Comissão'] = df['Valor Comissão'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor líquido'] = df['Valor líquido'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['Valor Rejeitado'] = df['Valor Rejeitado'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df = df.drop('Nro. Ref. Interna', axis=1)
    return df


# Extrai os arquivos do menu Vendas Internas/Transações de pagamento (lado ir).
def extractIRFile(fileName) :
    df = pd.read_csv(fileName, delimiter=';', dtype=object, encoding='latin_1')
    #Slice only transaction rows
    df = df[df['Bandeira'].isnull() == False]
    df['Parcela'] = df['Parcela'].fillna(0).astype(int)
    df['Valor do Pagamento'] = df['Valor do Pagamento'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor líquido'] = df['Valor líquido'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor da Parcela'] = df['Valor da Parcela'].fillna('0')
    df['Valor Adicional 1'] = df['Valor Adicional 1'].fillna('0')
    df['Valor Adicional 2'] = df['Valor Adicional 2'].fillna('0')
    df['Valor da Diferença'] = df['Valor da Diferença'].fillna('0')
    df['Valor da Parcela'] = df['Valor da Parcela'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor Adicional 1'] = df['Valor Adicional 1'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor Adicional 2'] = df['Valor Adicional 2'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor da Diferença'] = df['Valor da Diferença'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)

    df = df.iloc[0:len(df), 0:33]
    return df


# Extrai os arquivos de cancelamentos do menu Cancelamentos e Chargebacks/Vendas Canceladas-Contestadas.
def extractCancelFile(fileName) :
    df = pd.read_csv(fileName, delimiter=';', dtype=object, encoding='latin_1')
    df = df[df['Bandeira'].isnull() == False]
    df = df.iloc[0:len(df), 0:33]
    df['Parcela'] = df['Parcela'].fillna(0).astype(int)
    df['Dt. Venda'] = df['Dt. Venda'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df['Data Captura'] = df['Data Captura'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df['Data do Cancelamento'] = df['Data do Cancelamento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df['Valor Bruto'] = df['Valor Bruto'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor Comissão'] = df['Valor Comissão'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor líquido'] = df['Valor líquido'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Vlr. Bruto Cancelamento'] = df['Vlr. Bruto Cancelamento'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Vlr. Líquido Cancelamento'] = df['Vlr. Líquido Cancelamento'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    return df

# Extrai os arquivos de movimetnação financeira do menu conciliacao > agenda fincanceira
def extractFinanceFile(fileName) :
    df = pd.read_csv(fileName, delimiter=';', dtype=object, encoding='latin_1', error_bad_lines=False)
    df = df[df['Histórico'].isnull() == False]

    df['Data'] = df['Data'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df['Valor Bruto'] = df['Valor Bruto'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor Comissão'] = df['Valor Comissão'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor Líquido Previsto'] = df['Valor Líquido Previsto'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor Líquido Realizado'] = df['Valor Líquido Realizado'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    return df

# Extrai os arquivos de fluxo de caixa do menu adquirente > fluxo de caixa
def extractCashFlowFile(fileName):
    df = pd.read_csv(fileName, delimiter=';', dtype=object, encoding='latin_1') # error_bad_lines=False --> ignora linhas com erro.
    df = df[df['Estabelecimento'].isnull() == False]

    df['Data de Vencimento'] = df['Data de Vencimento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df['Data do Lote de Venda'] = df['Data do Lote de Venda'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y").strftime('%Y-%m-%d'), na_action='ignore')
    df['Valor Bruto'] = df['Valor Bruto'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor Comissão'] = df['Valor Comissão'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Vendas Antec./Cedidas'] = df['Vendas Antec./Cedidas'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Cancelamentos'] = df['Cancelamentos'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Chargeback'] = df['Chargeback'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Outros Ajustes'] = df['Outros Ajustes'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor das Antecipações'] = df['Valor das Antecipações'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Descontos Antec./Cessões'] = df['Descontos Antec./Cessões'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor Previsto'] = df['Valor Previsto'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Valor Pago'] = df['Valor Pago'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Saldo'] = df['Saldo'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Vendas Cedidas'] = df['Vendas Cedidas'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)
    df['Cessões Avulsas'] = df['Cessões Avulsas'].map(lambda x: str(x).replace('.', '').replace(',', '.')).astype(float)

    return df

def extractLegacySaleFile():

    fileName = "D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/ComprasAtivasLegado_v1.csv"
    df = pd.read_csv(fileName, delimiter=',', dtype=object, encoding='UTF-8')

    df['numero_venda_ir'] = df['numero_venda_ir'].map(lambda x: str(x).replace('.', ''))
    df['venda_bilheteria_id'] = df['venda_bilheteria_id'].map(lambda x: str(x).replace('.', ''))
    df['id_usuario'] = df['id_usuario'].map(lambda x: str(x).replace('.', ''))
    df['id_evento'] = df['id_evento'].map(lambda x: str(x).replace('.', ''))
    df['id_produtor_evento'] = df['id_produtor_evento'].map(lambda x: str(x).replace('.', ''))
    df['data_compra'] = df['data_compra'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'), na_action='ignore')
    df['data_evento'] = df['data_evento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M").strftime('%Y-%m-%d %H:%M'), na_action='ignore')
    df['data_venda_completa'] = df['data_venda_completa'].astype(datetime)
    df['quantidade_ingressos'] = df['quantidade_ingressos'].astype(int)
    df['valor_taxa_conveniencia_total'] = df['valor_taxa_conveniencia_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_taxa_entrega_total'] = df['valor_taxa_entrega_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_juros_total'] = df['valor_juros_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_ingressos_ativos_total'] = df['valor_ingressos_ativos_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_compra_original_total'] = df['valor_compra_original_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['numero_parcelas'] = df['numero_parcelas'].astype(int)
    df['numero_cartao'] = df['numero_cartao'].map(lambda x: str(x).replace('X', '*'))

    return df

def extractIRCancelLegacySalesFile():
    fileName = "D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/ComprasCanceladasLegado_v1.csv"
    df = pd.read_csv(fileName, delimiter=',', dtype=object, encoding='UTF-8')

    df['numero_venda_ir'] = df['numero_venda_ir'].map(lambda x: str(x).replace('.', ''))
    df['venda_bilheteria_id'] = df['venda_bilheteria_id'].map(lambda x: x.replace('.', ''))
    df['numero_cancelamento_ir'] = df['numero_cancelamento_ir'].map(lambda x: x.replace('.', ''))
    df['cancelamento_bilheteria_id'] = df['cancelamento_bilheteria_id'].map(lambda x: x.replace('.', ''))
    df['id_usuario'] = df['id_usuario'].map(lambda x: str(x).replace('.', ''))
    df['id_evento'] = df['id_evento'].map(lambda x: x.replace('.', ''))
    df['id_produtor_evento'] = df['id_produtor_evento'].map(lambda x: x.replace('.', ''))
    df['data_compra'] = df['data_compra'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'), na_action='ignore')
    df['data_cancelamento'] = df['data_cancelamento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'), na_action='ignore')
    df['data_evento'] = df['data_evento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M").strftime('%Y-%m-%d %H:%M'), na_action='ignore')
    df['data_cancelamento_completa'] = df['data_cancelamento_completa'].astype(datetime)
    df['quantidade_ingressos'] = df['quantidade_ingressos'].astype(int)
    df['valor_taxa_conveniencia_total'] = df['valor_taxa_conveniencia_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_taxa_entrega_total'] = df['valor_taxa_entrega_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_juros_total'] = df['valor_juros_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_ingressos_cancelados_total'] = df['valor_ingressos_cancelados_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_compra_cancelada_total'] = df['valor_compra_cancelada_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_compra_origem_total'] = df['valor_compra_origem_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['numero_parcelas'] = df['numero_parcelas'].astype(int)
    df['numero_cartao'] = df['numero_cartao'].map(lambda x: str(x).replace('X', '*'))

    return df

def extractBiletoSaleFile():
    fileName = "D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/2017-11-0911-30ComprasAtivasBileto.csv"
    df = pd.read_csv(fileName, delimiter=',', dtype=object, encoding='UTF-8')

    df['numero_venda_ir'] = df['numero_venda_ir'].map(lambda x: str(x).replace('.', ''))
    df['venda_bilheteria_id'] = df['venda_bilheteria_id'].map(lambda x: str(x).replace('.', ''))
    df['id_usuario'] = df['id_usuario'].map(lambda x: str(x).replace('.', ''))
    df['id_evento'] = df['id_evento'].map(lambda x: str(x).replace('.', ''))
    df['id_produtor_evento'] = df['id_produtor_evento'].map(lambda x: str(x).replace('.', ''))
    df['data_compra'] = df['data_compra'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'), na_action='ignore')
    df['data_evento'] = df['data_evento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M").strftime('%Y-%m-%d %H:%M'), na_action='ignore')
    df['data_venda_completa'] = df['data_venda_completa'].astype(datetime)
    df['quantidade_ingressos'] = df['quantidade_ingressos'].astype(int)
    df['valor_taxa_conveniencia_total'] = df['valor_taxa_conveniencia_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_taxa_entrega_total'] = df['valor_taxa_entrega_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_juros_total'] = df['valor_juros_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_ingressos_ativos_total'] = df['valor_ingressos_ativos_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_compra_original_total'] = df['valor_compra_original_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['numero_parcelas'] = df['numero_parcelas'].astype(int)
    df['numero_cartao'] = df['numero_cartao'].map(lambda x: str(x).replace('X', '*'))

    return df

def extractCancelBiletoSalesFile():
    fileName = "D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/2017-11-0914-50ComprasCanceladasBileto.csv"
    df = pd.read_csv(fileName, delimiter=',', dtype=object, encoding='UTF-8')

    df['numero_venda_ir'] = df['numero_venda_ir'].map(lambda x: str(x).replace('.', ''))
    df['venda_bilheteria_id'] = df['venda_bilheteria_id'].map(lambda x: x.replace('.', ''))
    df['numero_cancelamento_ir'] = df['numero_cancelamento_ir'].map(lambda x: x.replace('.', ''))
    df['cancelamento_bilheteria_id'] = df['cancelamento_bilheteria_id'].map(lambda x: x.replace('.', ''))
    df['id_usuario'] = df['id_usuario'].map(lambda x: str(x).replace('.', ''))
    df['id_evento'] = df['id_evento'].map(lambda x: x.replace('.', ''))
    df['id_produtor_evento'] = df['id_produtor_evento'].map(lambda x: str(x).replace('.', ''))
    df['data_compra'] = df['data_compra'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'), na_action='ignore')
    df['data_cancelamento'] = df['data_cancelamento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'), na_action='ignore')
    df['data_evento'] = df['data_evento'].map(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M").strftime('%Y-%m-%d %H:%M'), na_action='ignore')
    df['data_cancelamento_completa'] = df['data_cancelamento_completa'].astype(datetime)
    df['quantidade_ingressos'] = df['quantidade_ingressos'].astype(int)
    df['valor_taxa_conveniencia_total'] = df['valor_taxa_conveniencia_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_taxa_entrega_total'] = df['valor_taxa_entrega_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_juros_total'] = df['valor_juros_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_ingressos_cancelados_total'] = df['valor_ingressos_cancelados_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_compra_cancelada_total'] = df['valor_compra_cancelada_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['valor_compra_original_total'] = df['valor_compra_original_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    df['numero_parcelas'] = df['numero_parcelas'].astype(int)
    df['numero_cartao'] = df['numero_cartao'].map(lambda x: str(x).replace('X', '*'))

    return df

# Busca o nome e tipos de arquivos de conciliação e retorna na forma de listas.
def getFileNameGroup(type):
    pasta = 'D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/'
    adquirenteFiles = []
    irFiles = []
    cancelamentoFiles = []
    movimentoFinanceiroFiles = []
    fluxoCaixaFiles = []

    caminhos = [os.path.join(pasta, nome) for nome in os.listdir(pasta)]
    arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
    csvFileNames = [arq for arq in arquivos if arq.lower().endswith(".csv")]

    # Caso não tenha nenhum arquivo a ser importado.
    if len(csvFileNames) == 0: return adquirenteFiles, irFiles, cancelamentoFiles

    csvFileNames = pd.DataFrame(csvFileNames)
    csvFileNames.columns = ['File']

    for fileName in csvFileNames['File']:
        if 'transacoes-venda' in fileName:
            adquirenteFiles.append(fileName)
        elif 'transacoes-pagamento' in fileName:
            irFiles.append(fileName)
        elif 'vendas-canceladas-contestadas' in fileName:
            cancelamentoFiles.append(fileName)
        elif 'movimentacao-financeira' in fileName:
            movimentoFinanceiroFiles.append(fileName)
        elif 'FluxoCaixa' in fileName:
            fluxoCaixaFiles.append(fileName)

    if type == 0:
        return adquirenteFiles
    elif type == 1:
        return irFiles
    elif type == 2:
        return cancelamentoFiles
    elif type == 3:
        return movimentoFinanceiroFiles
    elif type == 4:
        return fluxoCaixaFiles
    else:
        return
    #return adquirenteFiles, irFiles, cancelamentoFiles, movimentoFinanceiroFiles

# Move um arquivo para outro diretório.
def moveFile(fileName):
    return shutil.move(fileName, defaultPathProcessFile)

#print(extractCancelFiles().head())

####################################################################################################
## TESTE

#print(extractIRFiles().head(10))

#df = extractCancelBiletoSalesFile()
#print(df.columns)

############
#pasta = 'D:/Balbi/Clientes/IngressoRapido/Conciliacao/Dev/python/conciliacao/dados/'
#caminhos = [os.path.join(pasta, nome) for nome in os.listdir(pasta)]
#arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
#csvFileNames = [arq for arq in arquivos if arq.lower().endswith(".csv")]

#print(csvFileNames)
#csvFileNames = pd.DataFrame(csvFileNames)
#print(csvFileNames.count())
#print(csvFileNames.head())
#csvFileNames.columns = ['File']


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