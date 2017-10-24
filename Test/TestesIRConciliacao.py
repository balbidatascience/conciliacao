import Services.DataBaseRepository as repository
from Services import EqualsFileRepository

def LoadDsAdquirenteFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    filesAdquirente, filesIR, filesCancelamento = EqualsFileRepository.getFileNameGroup()

    for fileName in filesAdquirente:
        print(fileName)
        df = EqualsFileRepository.extractAdquirenteFile(fileName)
        repository.insertDsTransacaoAdquirente(cursor, conn, df)
        EqualsFileRepository.moveFile(fileName)

    return True;

def LoadDsIRFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    filesAdquirente, filesIR, filesCancelamento = EqualsFileRepository.getFileNameGroup()

    for fileName in filesIR:
        print(fileName)
        df = EqualsFileRepository.extractIRFile(fileName)
        repository.insertDsTransacaoIR(cursor, conn, df)
        EqualsFileRepository.moveFile(fileName)

    return True;

def LoadDsCancelFiles():
    conn = repository.openConn()
    cursor = repository.openCursor(conn)
    filesAdquirente, filesIR, filesCancelamento = EqualsFileRepository.getFileNameGroup()

    for fileName in filesCancelamento:
        print(fileName)
        df = EqualsFileRepository.extractCancelFile(fileName)
        repository.insertDsCancelamento(cursor, conn, df)
        EqualsFileRepository.moveFile(fileName)

    return True;

def runETL():
    LoadDsAdquirenteFiles()
    LoadDsIRFiles()
    LoadDsCancelFiles()
    return True;

#---------------------------------------------------------
# TESTES


runETL()

#LoadDsAdquirenteFiles()
#LoadDsIRFiles()
#LoadDsCancelFiles()

#LoadDsTransacaoAdquirentes()
