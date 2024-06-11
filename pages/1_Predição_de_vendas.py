# Cria os insights sobre ocorrências de acidentes aeronáuticos

# importa as bibliotecas
from PIL import Image
import pandas as pd
import streamlit as st
import array as np
import sklearn
import joblib
from numpy import int64 as npint64
from fc import fc_download_s3 as ds3
import findspark
from pyspark.sql import SparkSession

# define a exibição total de linhas e colunas na exibição
#pd.set_option('display.max_rows', None)
#pd.set_option('display.max_columns', None)
import warnings
warnings.simplefilter('ignore')

# variáveis globais
dir_dados_tratados = 'arquivos_tratados'
dir_modelos = 'modelos'
arq_dados_tratados = 'df_supermarket_sales.csv' 
arq_modelos = 'dtree_model_vendasSupermarket.pkl'
    
# cria uma sessão spark
findspark.init('c:/spark') 
spk_session = SparkSession.builder.appName('my_app').getOrCreate()

# faz a leitura do arquivo e coloca na memória
@st.cache_data
def le_arquivo_vendas():
    global dir_dados_tratados
    global arq_dados_tratados 
    return pd.read_csv(dir_dados_tratados + barra + arq_dados_tratados)

# faz a leitura do arquivo e coloca na memória
@st.cache_data
def le_arquivo_modelo():
    global dir_modelos
    global arq_modelos    
    return joblib.load(dir_modelos + barra + arq_modelos)
    
#------------------- INÍCIO

# ******************** INICIALIZA VARIÁVEIS DO REPOSITÓRIO DE DADOS
dir_dados = 'arquivos'
dir_par = 'parametros'
dir_S3 = 'vendas-supermarket-s3'
dir_S3_tratados = 'vendas-supermarket-s3-tratados'
dir_S3_modelos = 'vendas-supermarket-s3-modelos'
arq_dados = 'X_supermarket_sales.csv'
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

# baixa arquivo de vendas tratado
retorno = 'ok'
#**************** comentado para que o kernel não trave se não existir um bucket s3 criado
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

# baixa arquivo do modelo de regressão para predição
#**************** comentado para que o kernel não trave se não existir um bucket s3 criado
#retorno = ds3.download_s3(
#            dir_S3_modelos, 
#            arq_modelos,
#            dir_modelos + barra + arq_modelos, 
#            access_key, secret_key, regiao)

if retorno != 'ok':
    print(retorno)
    print('bucket s3 => ' + dir_S3_modelos + ' arquivo => ' + arq_modelos + 
            ' ***** não foi baixado')
    exit()    

# le arquivo de vendas
df_vendas = le_arquivo_vendas()

# le o arquivo de modelo para fazer predição
Dtree_model = le_arquivo_modelo()

# cria df da sessão spark com o conteúdo de df_vendas
sdf = spk_session.createDataFrame(df_vendas)

xx = spk_session.read.csv(dir_dados_tratados + barra + arq_dados_tratados, header=True)
xxxx = xx.toPandas()
xx.show()
print(xxxx)

#----------------------------------------------
# Barra lateral sidebar
#----------------------------------------------
#-------- define a cor
st.markdown("""
<style>
    [data-testid=stSidebar] {
        background-color: #CBCBC8;
    }
</style>
""", unsafe_allow_html=True)

#-------- Carrega imagem
image_path = './images/predict_vendas.png'
ix = Image.open( image_path ) 
st.sidebar.image( ix, width=240 )

#-------- Empresa
st.sidebar.markdown('###### Powered by Jairo Bernardes Júnior')
#st.sidebar.markdown( '##### **Site em constante evolução')

#----------------------------------------------
# gráficos
#----------------------------------------------
#-------- Abas de finalidade dos dados
tab1, tab2, tab3 = st.tabs( ['Predição', '*****', '*****'])

#-------- Visão Estratégica
with tab1:
    with st.container():
        # Faz a predição da quantidade que será vendida por produto e dia da semana
        st.header( 'Faz a predição da quantidade que será vendida por produto e dia da semana' )

        dfx = df_vendas[['linha_produto_nro', 'mes', 'dia', 'preco_unitario', 'custo', 'dia_semana', 'feriado']]
        dfx2 = df_vendas[['linha_produto', 'qtde', 'linha_produto_nro', 'mes', 'dia', 'preco_unitario', 'custo', 'dia_semana', 'feriado']]

        st.write(sdf)

        dfx['preco_unitario'] = dfx['preco_unitario'] * 0.10
        qtde_vds = Dtree_model.predict(dfx)
        st.write(qtde_vds)
        exit()


        #[73.00, 435.66, 21.783, 0, 0, 1, 0, 5, 1]
        #X_enter = [2.00, 1.66, 21.783, 0, 0, 1, 0, 5, 1]
        #X_enter = pd.DataFrame(X_enter)
        #X_enter = X_enter.T
        #X_enter2 = [5.00, 435.66, 21.783, 0, 0, 1, 0, 5, 1]
        #X_enter2 = pd.DataFrame(X_enter2)        
        #X_enter2 = X_enter2.T

        #X_enter.columns = ['preco_unitario', 'custo', 'imposto','dia_semana', 'feriado', 'tipo_cliente_nro', \
        #                   'genero_nro', 'linha_produto_nro', 'moeda_nro']
        #X_enter2.columns = ['preco_unitario', 'custo', 'imposto','dia_semana', 'feriado', 'tipo_cliente_nro', \
        #                   'genero_nro', 'linha_produto_nro', 'moeda_nro']     

        #X_enter = pd.concat([X_enter, X_enter2], axis=0)   
        
        #X_enter['dia_semana'] = X_enter['dia_semana'].astype(npint64)
        #X_enter['feriado'] = X_enter['feriado'].astype(npint64)
        #X_enter['tipo_cliente_nro'] = X_enter['tipo_cliente_nro'].astype(npint64)
        #X_enter['genero_nro'] = X_enter['genero_nro'].astype(npint64)
        #X_enter['linha_produto_nro'] = X_enter['linha_produto_nro'].astype(npint64)
        #X_enter['moeda_nro'] = X_enter['moeda_nro'].astype(npint64)

        #print(X_enter.info())
        
        #st.write(X_enter)

        #qtde_vds = Dtree_model.predict(X_enter)
        #st.write(qtde_vds)