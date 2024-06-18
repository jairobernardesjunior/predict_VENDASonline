# Cria os insights sobre ocorrências de acidentes aeronáuticos

# importa as bibliotecas
from PIL import Image
from numpy import int64 as npint64
from pyspark.sql import SparkSession
from pandasql import sqldf
from datetime import datetime as dt

import pandas as pd
import streamlit as st
import array as np
import sklearn
import joblib
import boto3
import botocore
import findspark
import requests
import plotly.express as px

# define a exibição total de linhas e colunas na exibição
#pd.set_option('display.max_rows', None)
#pd.set_option('display.max_columns', None)
import warnings
warnings.simplefilter('ignore')

# variáveis globais
dir_dados_tratados = 'arquivos_tratados'
dir_modelos = 'modelos'
arq_dados_tratados = 'X_supermarket_sales.csv' 
arq_modelos = 'dtree_model_vendasSupermarket.pkl'
dfg_referencia = pd.DataFrame()
    
# cria uma sessão spark
findspark.init('c:/spark') 
spk_session = SparkSession.builder.appName('my_app').getOrCreate()

# faz o download de arquivo armazenado em bucket s3 da aws
def download_s3(nome_buckets3, nome_arquivo, path_arquivo, access_key, secret_key, regiao):
    client = boto3.client(
        service_name= 's3',
        aws_access_key_id= access_key,
        aws_secret_access_key= secret_key,
        region_name= regiao # voce pode usar qualquer regiao
        )    

    retorno = ''

    try:
        client.download_file(nome_buckets3, nome_arquivo, path_arquivo)
        retorno = 'ok'
    except botocore.exceptions.ClientError as e:
        # if e.response['Error']['Code'] == "404":
        retorno = e.response['Error']

    return retorno

# faz leitura do arquivo de vendas consumindo a api 'vendas' do api_server
@st.cache_data
def le_arquivo_vendas_consome_api():
    url = ' http://127.0.0.1:5000/vendas/'

    #url = ' http://localhost:5000/vendas/'

    df_vendas = requests.get(url)
    df_vendas = pd.DataFrame(df_vendas)
    
    df_vendas['linha_produto_nro'] = df_vendas['linha_produto_nro'].astype(npint64)
    df_vendas['ano'] = df_vendas['ano'].astype(npint64)
    df_vendas['mes'] = df_vendas['mes'].astype(npint64)
    df_vendas['dia'] = df_vendas['dia'].astype(npint64)
    df_vendas['preco_unitario'] = df_vendas['preco_unitario'].astype(float)
    df_vendas['custo'] = df_vendas['custo'].astype(float)  
    df_vendas['dia_semana'] = df_vendas['dia_semana'].astype(npint64)  
    df_vendas['feriado'] = df_vendas['feriado'].astype(npint64)
    df_vendas['qtde'] = df_vendas['qtde'].astype(float)

    return df_vendas

# faz leitura do arquivo de vendas usando a sessão do spark
@st.cache_data
def le_arquivo_vendas_spark():
    global dir_dados_tratados
    global arq_dados_tratados 
    df_vendas = spk_session.read.csv(dir_dados_tratados + barra + arq_dados_tratados, header=True)
    df_vendas = df_vendas.toPandas()

    df_vendas['linha_produto_nro'] = df_vendas['linha_produto_nro'].astype(npint64)
    df_vendas['ano'] = df_vendas['ano'].astype(npint64)
    df_vendas['mes'] = df_vendas['mes'].astype(npint64)
    df_vendas['dia'] = df_vendas['dia'].astype(npint64)
    df_vendas['preco_unitario'] = df_vendas['preco_unitario'].astype(float)
    df_vendas['custo'] = df_vendas['custo'].astype(float)  
    df_vendas['dia_semana'] = df_vendas['dia_semana'].astype(npint64)  
    df_vendas['feriado'] = df_vendas['feriado'].astype(npint64)
    df_vendas['qtde'] = df_vendas['qtde'].astype(float)

    return df_vendas

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

# encontra o mês em formato numérico
def encontra_mes_nro(xmes, list_mes):
    mes_nro = 1
    for nome_mes in list_mes:
        if xmes == nome_mes:
            return mes_nro
        mes_nro += 1

    st.markdown(f'<h1 style="color:#e32636;font-size:40px;">{"Mês nro não encontrado"}</h1>', unsafe_allow_html=True)
    exit()
    
#------------------- INÍCIO

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

# variáveis gerais
list_mes = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro',
     'Outubro', 'Novembro', 'Dezembro']
list_dias = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 
     25, 26, 27, 28, 29, 30, 31]
list_dia_semana = ['segunda-feira', 'terça-feira', 'quarta-feira', 'quinta-feira', 'sexta-feira', 
                   'sábado', 'domingo']

# ******************** CHAMA A FUNÇÃO DE DOWNLOAD DO BUCKET S3 para baixar os arquivos tratados

# baixa arquivo de vendas tratado
retorno = 'ok'
#**************** comentado para que o kernel não trave se não existir um bucket s3 criado
#retorno = download_s3(
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

# le arquivo de vendas com pandas
df_vendas = le_arquivo_vendas()

# le arquivo de vendas usando a sessão do spark
#df_vendas = le_arquivo_vendas_spark()

# faz leitura do arquivo de vendas consumindo a api 'vendas' do api_server
#df_vendas = le_arquivo_vendas_consome_api()

# le o arquivo de modelo para fazer predição
Dtree_model = le_arquivo_modelo()

# encontra lista de anos no df_vendas
df_ano = sqldf('select ano, max(ano), max(preco_unitario)  from df_vendas  group by ano  order by ano desc')
ano_default = df_ano.loc[0][0]
list_ano = df_ano['ano'].values.tolist()

# encontra lista de produtos no df_vendas
df_produto = sqldf('select linha_produto, max(linha_produto) '\
               '    from df_vendas '\
               '    group by linha_produto '\
               '    order by linha_produto'
               )
produto_defaut = df_produto.loc[0][0]
list_produto = df_produto['linha_produto'].values.tolist()

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

#-------- cria objeto para selecionar o ano de referência
xano = st.sidebar.multiselect(
    'ANO de referência (anterior ao atual)',
    list_ano,
    default=[ano_default]
    )

#-------- cria objeto para selecionar o produto para a predição
xproduto = st.sidebar.multiselect(
    'Produto para a predição',
    list_produto,
    default=[produto_defaut]
    )

#-------- cria objeto para selecionar o mês da predição
xmes = st.sidebar.multiselect(
    'Mês para a predição',
    list_mes,
    default=['Janeiro']
    )

#-------- cria objeto para selecionar o dia da predição
xdia = st.sidebar.multiselect(
    'Dia para a predição',
    list_dias,
    default=[1]
    )

st.markdown(f'<h1 style="color:#e32636;font-size:10px;">{""}</h1>', unsafe_allow_html=True)
if len(xano) != 1:
    st.markdown(f'<h1 style="color:#e32636;font-size:40px;">{"Ano de referência não informado ou informado mais de uma vez"}</h1>', unsafe_allow_html=True)
    exit()

if len(xproduto) != 1:
    st.markdown(f'<h1 style="color:#e32636;font-size:40px;">{"Produto não informado ou informado mais de uma vez"}</h1>', unsafe_allow_html=True)
    exit()

if len(xmes) != 1:
    st.markdown(f'<h1 style="color:#e32636;font-size:40px;">{"Mês não informado ou informado mais de uma vez"}</h1>', unsafe_allow_html=True)
    exit()

if len(xdia) != 1:
    st.markdown(f'<h1 style="color:#e32636;font-size:40px;">{"Dia não informado ou informado mais de uma vez"}</h1>', unsafe_allow_html=True)
    exit()              

#-------- Empresa
st.sidebar.markdown('#### Powered by Jairo Bernardes Júnior')
#st.sidebar.markdown( '##### **Site em constante evolução')

#-------- captura o nro do dia da semana para a data de predição
df_dia_semana = pd.DataFrame({'data_predicao': ['01-01-2000'], 'dia_semana_predicao': [0]})
mes_nro = encontra_mes_nro(xmes[0], list_mes)

date_format = '%Y-%m-%d %H:%M:%S'

data_atual = dt.today()
ano_atual = data_atual.year
data_pred =  \
    "{:04d}".format(ano_atual) + '-' + "{:02d}".format(mes_nro) + '-' +"{:02d}".format(xdia[0]) + ' 01:01:01' 
data_pred = dt.strptime(data_pred, date_format)      

df_dia_semana['data_predicao'] = data_pred
df_dia_semana['dia_semana_predicao'] = df_dia_semana['data_predicao'].dt.weekday
dia_semana_nro = df_dia_semana['dia_semana_predicao'].unique()[0]

list_id = [0]
linhas = df_vendas.shape[0]

for i in range(1, linhas, 1):
    list_id.append(i)

df_id = pd.DataFrame(list_id)
df_id.columns = ['id']
df_vendas = pd.concat([df_id, df_vendas], axis=1)

# seleciona dados do dia da semana mais recente dentro do mês escolhido e do ano de referência
dfx = sqldf(f'select ano, mes, linha_produto, dia, linha_produto_nro, \
                   preco_unitario, custo, dia_semana, \
                   feriado, qtde \
                        from df_vendas \
                        where ano = {xano[0]} and \
                            mes = {mes_nro} and \
                            dia_semana = {dia_semana_nro} and \
                            linha_produto = "{xproduto[0]}" and \
                            dia = (select dia \
                                        from df_vendas \
                                        where ano = {xano[0]} and \
                                            mes = {mes_nro} and \
                                            dia_semana = {dia_semana_nro} and \
                                            linha_produto = "{xproduto[0]}" \
                                        group by dia \
                                        order by dia desc \
                                        limit 1) \
                    order by ano, mes'
               )

# vamos duplicar os registros apenas para resultado de teste
dfx = pd.concat([dfx, dfx, dfx, dfx], axis=0)
dfx = dfx.reset_index()

if len(dfx) < 1:
    st.markdown(f'<h1 style="color:#e32636;font-size:40px;">{"Não existe dados de referência para os parâmetros selecionados"}</h1>', unsafe_allow_html=True)
    exit() 

dfx2 = dfx[['ano', 'mes', 'dia', 'linha_produto', 'preco_unitario', 'custo', 'dia_semana', 'qtde']]\
    .sort_values(['ano', 'mes', 'dia', 'linha_produto'], ascending=False)
dfx = dfx[['linha_produto_nro', 'mes', 'dia', 'preco_unitario', 'custo', 'dia_semana', 'feriado']]

preco_default = dfx.loc[0][3]
dia_ref = dfx2.loc[0][2]

data_ref = \
    "{:04d}".format(xano[0]) + '-' + "{:02d}".format(mes_nro) + '-' +"{:02d}".format(dia_ref) + ' 01:01:01' 
data_ref = dt.strptime(data_ref, date_format)

#if data_ref > data_pred:
#    st.markdown(f'<h1 style="color:#e32636;font-size:40px;">{"Data de predição menor que a data de referência"}</h1>', unsafe_allow_html=True)
#    exit()  
#if data_atual > data_pred:
#    st.markdown(f'<h1 style="color:#e32636;font-size:40px;">{"Data atual maior que a data de predição"}</h1>', unsafe_allow_html=True)
#    exit()

#----------------------------------------------
# gráficos
#----------------------------------------------
#-------- Abas de finalidade dos dados
tab1, tab2, tab3 = st.tabs( ['Predição de Vendas', 'Vendas Anteriores', '*****'])

#-------- Visão Estratégica
with tab1:
    with st.container():
        # Faz a predição da quantidade que será vendida por produto e dia da semana
        st.markdown('##### Predição da quantidade que será vendida por produto e dia da semana no ano corrente' )   

        #st.write('data atual', data_atual, 'data predição', data_pred, 'data referência', data_ref)

        st.header(xproduto[0])

        #-------- cria objeto para receber o preço unitário de venda a ser praticado
        xpreco_unitario = st.number_input("Preço Unitário de Venda que será praticado (R$)", 
                                value=preco_default, step=0.5, format="%0.2f")  

        dfx['preco_unitario'] = xpreco_unitario
        qtde_vds = Dtree_model.predict(dfx)

        qtde_vds = pd.DataFrame(qtde_vds)
        qtde_vds.columns = ['qtde']
        qtde_vds = qtde_vds['qtde'].sum()

        ydia = "% s" % xdia[0]
        st.write('Para o preço unitário de', xpreco_unitario,
                 '. A previsão de vendas é de', qtde_vds, 
                 'unidades em', xdia[0], ' de ', xmes[0],
                 ' - ', list_dia_semana[dia_semana_nro], ' - ', data_pred.strftime('%d/%m/%Y'))

        st.markdown('##### referência:' )
        st.write(dfx2)       

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

with tab2:
    with st.container():
        # Percentual de valor unitário e vendas
        st.header( 'Percentual de quantidade vendida por valor unitário de venda em ')             
        st.write('data referência', data_ref)

        # passa os dados para o gráfico e exibe
        fig = px.pie( dfx2, values='qtde', names='preco_unitario', title='quantidade vendida por valor unitário')
        fig.update_layout(autosize=False)
        st.plotly_chart( fig, use_container_width=True )  