import Services.DataBaseRepository as repository
from Services import EqualsFileRepository
import numpy as np

def LoadDsAdquirenteFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    filesAdquirente = EqualsFileRepository.getFileNameGroup(0)

    for fileName in filesAdquirente:
        print(fileName)
        df = EqualsFileRepository.extractAdquirenteFile(fileName)
        repository.insertDsTransacaoAdquirente(cursor, conn, df)
        EqualsFileRepository.moveFile(fileName)

    return True;

def LoadDsIRFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    filesIR = EqualsFileRepository.getFileNameGroup(1)

    for fileName in filesIR:
        print(fileName)
        df = EqualsFileRepository.extractIRFile(fileName)
        repository.insertDsTransacaoIR(cursor, conn, df)
        EqualsFileRepository.moveFile(fileName)

    return True;

def LoadDsCancelFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    filesCancelamento = EqualsFileRepository.getFileNameGroup(2)

    for fileName in filesCancelamento:
        print(fileName)
        df = EqualsFileRepository.extractCancelFile(fileName)
        repository.insertDsCancelamento(cursor, conn, df)
        EqualsFileRepository.moveFile(fileName)

    return True

def LoadDsFinanceFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    financeFiles = EqualsFileRepository.getFileNameGroup(3)

    for fileName in financeFiles:
        print(fileName)
        df = EqualsFileRepository.extractFinanceFile(fileName)
        repository.insertDsMovimentoFinanceiro(conn, cursor, df)
        EqualsFileRepository.moveFile(fileName)
    return True

def LoadCashFlowFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    cashFlowFiles = EqualsFileRepository.getFileNameGroup(4)
    for fileName in cashFlowFiles:
        print(fileName)
        df = EqualsFileRepository.extractCashFlowFile(fileName)
        repository.saveCashFlow(conn, cursor, df)
        EqualsFileRepository.moveFile(fileName)
    return True

def LoadLegacySaleFile():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    saleIRFile = EqualsFileRepository.extractLegacySaleFile()

    repository.saveLegacySale(conn=conn, cursor=cursor, df=saleIRFile)
    return True

def LoadIRCancelLegacySaleFile():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    saleIRFile = EqualsFileRepository.extractIRCancelLegacySalesFile()

    repository.saveIRCancelLegacySale(conn=conn, cursor=cursor, df=saleIRFile)
    return True

def LoadBiletoSaleFile():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    saleIRFile = EqualsFileRepository.extractBiletoSaleFile()

    repository.saveBiletoSale(conn=conn, cursor=cursor, df=saleIRFile)
    return True

def LoadCancelBiletoSaleFile():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    df = EqualsFileRepository.extractCancelBiletoSalesFile()

    repository.saveCancelBiletoSale(conn=conn, cursor=cursor, df=df)
    return True

def runETL():
    LoadDsAdquirenteFiles()
    LoadDsIRFiles()
    LoadDsCancelFiles()
    LoadCashFlowFiles()
    return True;



#---------------------------------------------------------
# TESTES

#LoadCashFlowFiles()
#LoadBiletoSaleFile()
#LoadCancelBiletoSaleFile()
#LoadIRCancelLegacySaleFile()

#LoadDsAdquirenteFiles()
#LoadDsIRFiles()
#LoadDsCancelFiles()

#LoadDsTransacaoAdquirentes()
