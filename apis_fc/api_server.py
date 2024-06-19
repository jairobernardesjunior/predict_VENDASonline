# cria sessão spark e faz leitura do arquivo de vendas
# o objetivo dessa api é ler e entregar, no seu consumo, dados de vendas já tratados e formatados 
# para serem usados pelo modelo de predição de quantidade de vendas em supermercado, tudo isso 
# utilizando os recursos do spark, criando uma sessão para isso.

# importa as bibliotecas
from flask import Flask, make_response, jsonify
import fc_download_s3 as ds3
from pyspark.sql import SparkSession
import findspark

app = Flask(__name__)
app.config['DEBUG'] = True

# variáveis globais
dir_dados_tratados = 'arquivos_tratados'
arq_dados_tratados = 'X_supermarket_sales.csv' 
barra = '/'

# cria uma sessão spark
findspark.init('c:/spark') 
spk_session = SparkSession.builder.appName('my_app').getOrCreate()

# le arquivo de vendas usando a sessão do spark
def le_arquivo_vendas():
    global dir_dados_tratados
    global arq_dados_tratados 
    global barra
    return spk_session.read.csv(dir_dados_tratados + barra + arq_dados_tratados, header=True)

@app.route('/vendas/', methods=['GET'])
def get_vendas():

    # ******************** INICIALIZA VARIÁVEIS DO REPOSITÓRIO DE DADOS
    dir_dados = 'arquivos'
    dir_par = 'parametros'
    dir_S3 = 'vendas-supermarket-s3'
    dir_S3_tratados = 'vendas-supermarket-s3-tratados'
    dir_S3_modelos = 'vendas-supermarket-s3-modelos'
    arq_dados = 'df_supermarket_sales.csv'
    arq_keys = 'parametros/chaves_acesso.txt'
    barra = '/'    

    # ******************** LE O ARQUIVO DE CHAVES
    arq_par = open(arq_keys,'r')

    access_key = arq_par.readline()
    access_key = access_key[0:len(access_key) -1]

    secret_key = arq_par.readline()
    secret_key = secret_key[0:len(secret_key) -1]

    regiao = arq_par.readline()
    arq_par.close()

    # ******************** CHAMA A FUNÇÃO DE DOWNLOAD DO BUCKET S3 para baixar os arquivos tratados

    #**************** comentado para que o kernel não trave se não existir um bucket s3 criado

    # baixa arquivo de vendas tratado
    retorno = 'ok'
    #retorno = ds3.download_s3(
    #            dir_S3_tratados, 
    #            arq_dados_tratados, 
    #            dir_dados_tratados + barra + arq_dados_tratados, 
    #            access_key, secret_key, regiao)

    if retorno != 'ok':
        print(retorno)
        print('bucket s3 => ' + dir_S3_tratados + ' arquivo => ' + arq_dados_tratados + 
                ' ***** não foi baixado')
        exit()

    # le arquivo de vendas
    sdf = le_arquivo_vendas()
    sdf = sdf.toPandas().head(5)

    return make_response(jsonify(sdf.values.tolist()))

app.run()