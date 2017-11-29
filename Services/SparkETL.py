from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DecimalType
from datetime import datetime

sparkContext = SparkContext('local')
session = SparkSession.builder.master('local').appName('ir-conciliacao').config("spark.some.config.option", "some-value").getOrCreate()
sqlContext = SQLContext(sparkContext)

# def Fase de extracao:
sales = session.read.csv('D:/Balbi/IR/Files/2017-11-09 11-30 Compras ativas da Bileto.csv', header=True)
sales.cache()

# Print o schema:
#sales.printSchema()
#print(sales.take(5))

# Funcoes definidas:
udfClearPoint = UserDefinedFunction(lambda x: str(x).replace('.', ''), StringType())
udfFormatDate = UserDefinedFunction(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'), StringType())
udfFormatDate2 = UserDefinedFunction(lambda x: datetime.strptime(str(x), "%d/%m/%Y %H:%M:%S"), TimestampType())
udfFormatDecimal = UserDefinedFunction(lambda x: str(x).replace('.', '').replace(',', '.'), StringType())


    #df['valor_taxa_conveniencia_total'] = df['valor_taxa_conveniencia_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    #df['valor_taxa_entrega_total'] = df['valor_taxa_entrega_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    #df['valor_juros_total'] = df['valor_juros_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    #df['valor_ingressos_ativos_total'] = df['valor_ingressos_ativos_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    #df['valor_compra_original_total'] = df['valor_compra_original_total'].map(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    #df['numero_parcelas'] = df['numero_parcelas'].astype(int)
    #df['numero_cartao'] = df['numero_cartao'].map(lambda x: str(x).replace('X', '*'))

def TransformBiletoSale(sales):

    #print(type(sales))
    sales = sales.select(*[udfClearPoint(column).alias('numero_venda_ir') if column == 'numero_venda_ir' else column for column in sales.columns])
    sales = sales.select(*[udfClearPoint(column).alias('venda_bilheteria_id') if column == 'venda_bilheteria_id' else column for column in sales.columns])
    sales = sales.select(*[udfClearPoint(column).alias('id_usuario') if column == 'id_usuario' else column for column in sales.columns])
    sales = sales.select(*[udfClearPoint(column).alias('id_evento') if column == 'id_evento' else column for column in sales.columns])
    sales = sales.select(*[udfClearPoint(column).alias('id_produtor_evento') if column == 'id_produtor_evento' else column for column in sales.columns])

    sales = sales.select(*[udfFormatDecimal(column).alias('valor_taxa_conveniencia_total') if column == 'valor_taxa_conveniencia_total' else column for column in sales.columns])
    sales = sales.select(*[udfFormatDecimal(column).alias('valor_taxa_entrega_total') if column == 'valor_taxa_entrega_total' else column for column in sales.columns])
    sales = sales.select(*[udfFormatDecimal(column).alias('valor_juros_total') if column == 'valor_juros_total' else column for column in sales.columns])
    sales = sales.select(*[udfFormatDecimal(column).alias('valor_ingressos_ativos_total') if column == 'valor_ingressos_ativos_total' else column for column in sales.columns])
    sales = sales.select(*[udfFormatDecimal(column).alias('valor_compra_original_total') if column == 'valor_compra_original_total' else column for column in sales.columns])


    sales = sales.withColumn("data_compra", to_timestamp(sales["data_compra"], 'dd/MM/yyyy HH:mm:ss'))
    sales = sales.withColumn("data_evento", to_timestamp(sales["data_evento"], 'dd/MM/yyyy HH:mm'))
    sales = sales.withColumn("data_venda_completa", sales["data_venda_completa"].astype(TimestampType()))
    sales = sales.withColumn("quantidade_ingressos", sales["quantidade_ingressos"].astype(IntegerType()))

    #sales = sales.withColumn("valor_taxa_conveniencia_total", sales["valor_taxa_conveniencia_total"].astype(DecimalType(10,2)))
    #sales = sales.withColumn("valor_taxa_entrega_total", sales["valor_taxa_entrega_total"].astype(DecimalType()))
    #sales = sales.withColumn("valor_juros_total", sales["valor_juros_total"].astype(DecimalType()))
    #sales = sales.withColumn("valor_ingressos_ativos_total", sales["valor_ingressos_ativos_total"].astype(DecimalType()))
    #sales = sales.withColumn("valor_compra_original_total", sales["valor_compra_original_total"].astype(DecimalType()))
    #sales = sales.withColumn("numero_parcelas", sales["numero_parcelas"].astype(IntegerType()))

    return sales

#rddSales_v1 = sales.rdd.map(lambda x: str(x).replace('.', ''))

#rdd1 = sales.select("numero_venda_ir").rdd.map(lambda x: str(x).replace('.', ''))
#sales['numero_venda_ir'] = sales['numero_venda_ir'].rdd.map(lambda x: str(x).replace('.', '')).


sales = TransformBiletoSale(sales)

print(sales.take(5))

#df = sqlContext.createDataFrame(rdd1)
#df.show()


#df.write.jdbc(url="jdbc:mysql://localhost/ir-sale", table="dsVendaAtivaBileto1", mode='Overwrite', properties=prop)

#=============================
# Salva o dataframe no BD
prop = {'user':'root','password':'@gabriel', 'driver':'com.mysql.jdbc.Driver'}
sales.write.jdbc(url="jdbc:mysql://localhost/ir-sale", table="dsVendaAtivaBileto", mode='Overwrite', properties=prop)


#print(vendaID.first())
#sales.printSchema()
#sales.select('numero_venda_ir').show()
#sales.show()

#sales.persist()

#print(sales.first())
#print(type(sales))
#print(sales.head())

# Tranformar as linhas via spark.



# Transforma um rdd em dataframe do panda.
#df = sales.collect()
#df = pd.DataFrame(df)
#print(df.head())


# Connection with MySql
#mysql_df = session.read.format("jdbc").options(
#    url = "jdbc:mysql://localhost/ir-sale",
#    driver = "com.mysql.jdbc.Driver",
#    dbtable = "dsvendaativabileto",
#    user = "root",
#    password = "@gabriel").load()