import Services.ConciliacaoRepository as repository
from Services import ColetaDadosConciliacao


def LoadDsTransacaoAdquirentes() :
    conn = repository.openConn();
    cursor = repository.openCursor(conn);

    df = ColetaDadosConciliacao.extractAdquirentesFiles();
    dfHead, dfRow = ColetaDadosConciliacao.SliceAdquirentesFilesHeaderAndRows(df);

    repository.insertDsTransacaoAdquirente(cursor, conn, dfRow);
    return True;

def LoadDsTransacaoIR():
    conn = repository.openConn();
    cursor = repository.openCursor(conn);
    dfHead, dfRow = ColetaDadosConciliacao.extractIRFiles();
    repository.insertDsTransacaoIR(cursor, conn, dfRow);
    return True;

def LoadDsCancelamento():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    df = ColetaDadosConciliacao.ExtractCanceledFiles()
    repository.InsertDsCancelamento(cursor=cursor, conn=conn, dfRows=df)
    return True;

#---------------------------------------------------------
# TESTES

LoadDsCancelamento()

#LoadDsTransacaoAdquirentes()
#LoadDsTransacaoIR()


#LoadDsTransacaoAdquirentes()
