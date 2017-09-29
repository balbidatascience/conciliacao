import Services.ConciliacaoRepository as repository
from Services import ColetaDadosConciliacao


def LoadDsTransacaoAdquirentes() :
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    df = ColetaDadosConciliacao.extractAdquirentesFiles()
    repository.insertDsTransacaoAdquirente(cursor, conn, df)
    return True

def LoadDsTransacaoIR():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    dfRow = ColetaDadosConciliacao.extractIRFiles()
    repository.insertDsTransacaoIR(cursor, conn, dfRow)
    return True

def LoadDsCancelamento():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    df = ColetaDadosConciliacao.extractCancelFiles()
    repository.InsertDsCancelamento(cursor=cursor, conn=conn, dfRows=df)
    return True

def LoadDsAdquirenteFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    filesAdquirente, filesIR, filesCancelamento = ColetaDadosConciliacao.getFileNameGroup()

    for fileName in filesAdquirente:
        df = ColetaDadosConciliacao.extractAdquirenteFile(fileName)
        repository.insertDsTransacaoAdquirente(cursor, conn, df)
        ColetaDadosConciliacao.moveFile(fileName)

    return True;

def LoadDsIRFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    filesAdquirente, filesIR, filesCancelamento = ColetaDadosConciliacao.getFileNameGroup()

    for fileName in filesIR:
        print(fileName)
        df = ColetaDadosConciliacao.extractIRFile(fileName)
        repository.insertDsTransacaoIR(cursor, conn, df)
        ColetaDadosConciliacao.moveFile(fileName)

    return True;

#---------------------------------------------------------
# TESTES

#LoadDsAdquirenteFiles()
LoadDsIRFiles()

#LoadDsCancelamento()

#LoadDsTransacaoAdquirentes()
#LoadDsTransacaoIR()


#LoadDsTransacaoAdquirentes()
