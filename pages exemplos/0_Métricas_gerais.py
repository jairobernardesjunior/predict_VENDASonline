# Cria os insights sobre ocorrências de acidentes aeronáuticos

# importa as bibliotecas
from haversine import haversine
from datetime import datetime
from PIL import Image
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st
import plotly.express as px
import folium

# define a exibição total de linhas e colunas na exibição
#pd.set_option('display.max_rows', None)
#pd.set_option('display.max_columns', None)
#import warnings
#warnings.simplefilter('ignore')

# faz a leitura do arquivo e coloca na memória
@st.cache_data
def le_arquivo_analise_ocorr():
    df_ocorrencias = pd.read_csv( 'dataset_analise/df_acidentes_analise_ocorr.csv' )
    df_ocorrencias['ocorrencia_dia'] = pd.to_datetime(df_ocorrencias['ocorrencia_dia'])
    return df_ocorrencias

@st.cache_data
def le_arquivo_analise_aero():
    df_aeronaves = pd.read_csv( 'dataset_analise/df_acidentes_analise_aero.csv' )
    df_aeronaves['ocorrencia_dia'] = pd.to_datetime(df_aeronaves['ocorrencia_dia'])
    return df_aeronaves

#------------------- INÍCIO
df_ocorrencias = le_arquivo_analise_ocorr()
df_aeronaves = le_arquivo_analise_aero()

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
image_path = './images/aviao1.jpg'
ix = Image.open( image_path ) 
st.sidebar.image( ix, width=240 )

#-------- Empresa
st.sidebar.markdown( '### Powered by Jairo Bernardes Júnior')
st.sidebar.markdown( '##### **Site em constante evolução')

#----------------------------------------------
# Layout de dados
#----------------------------------------------
#-------- Dados Gerais - monta dados gerais relativos a ocorrências
with st.container():
    st.header( 'Métricas Gerais' )

    col1, col2 = st.columns( 2, gap='Large')

    with col1:
        #  data inicial
        col1.metric( 'Data Inicial', df_ocorrencias.loc[:, 'ocorrencia_dia'].min().strftime("%d/%m/%Y") )            

    with col2:
        # data final
        col2.metric( 'Data Final', df_ocorrencias.loc[:, 'ocorrencia_dia'].max().strftime("%d/%m/%Y") ) 

with st.container():

    col1, col2, col3 = st.columns( 3, gap='Large')
    with col1:
        # total de ocorrências
        col1.metric( 'Total de Ocorrências', df_ocorrencias.loc[:, 'ocorrencia_classificacao'].count() )            

    with col2:
        # total de aeródromos envolvidos
        col2.metric( 'Qtde de Aeródromos', len(pd.unique(df_ocorrencias['ocorrencia_aerodromo'])) )   

    with col3:
        # total de saída de pista
        col3.metric( 'Qtde de Saídas da Pista', df_ocorrencias.loc[:, 'qtde_saip_total'].max() ) 

with st.container():

    col1, col2, col3 = st.columns( 3, gap='Large')
    with col1:
        # total de aeronaves envolvidas
        col1.metric( 'Total de Aeronaves', len(pd.unique(df_aeronaves['aeronave_matricula'])) )               

    with col2:
        # total de modelos envolvidos
        col2.metric( 'Qtde de Modelos', len(pd.unique(df_aeronaves['aeronave_modelo'])) )   

    with col3:
        # total de tipos de veículo
        col3.metric( 'Qtde de Tipos de Veículos', len(pd.unique(df_aeronaves['aeronave_tipo_veiculo'])) )           