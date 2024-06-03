# Cria os insights sobre fatores contribuintes e recomendações de segurança em acidentes aeronáuticos

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
def le_arquivo_analise():
    df_fator_recomendacao = pd.read_csv( 'dataset_analise/df_acidentes_analise_fator_recomendacao.csv' )
    df_fator_recomendacao['ocorrencia_dia'] = pd.to_datetime(df_fator_recomendacao['ocorrencia_dia'])
    return df_fator_recomendacao

# monta dataframe de fatores contribuintes
@st.cache_data
def seleciona_fator_nome(dfx):
    return dfx[['fator_nome', 'qtde_fator_nome', 'perc_fator_nome']].\
        drop_duplicates().sort_values('qtde_fator_nome', ascending=False)

# monta dataframe de áreas
@st.cache_data
def seleciona_fator_area(dfx):
    return dfx[['fator_area', 'qtde_area', 'perc_area']].\
        drop_duplicates().sort_values('qtde_area', ascending=False)

#------------------- INÍCIO
df_fator_recomendacao = le_arquivo_analise()

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
image_path = './images/aviao4.jpg'
ix = Image.open( image_path ) 
st.sidebar.image( ix, width=240 )

#-------- Empresa
st.sidebar.markdown( '### Powered by Jairo Bernardes Júnior')
st.sidebar.markdown( '##### **Site em constante evolução')

#----------------------------------------------
# gráficos
#----------------------------------------------
#-------- Abas de finalidade dos dados
tab1, tab2, tab3 = st.tabs( ['Visão Estratégica', 'Visão Tática', '-'])

#-------- Visão Estratégica
with tab1:
    with st.container():
        # Percentual de Fatores Contribuintes
        st.header( 'Percentual de Fatores Contribuintes' )   

        # carrega o dataframe
        dfx = seleciona_fator_nome(df_fator_recomendacao)

        #-------- Controle de dados dupla face  - define intervalo online
        intervalo = st.slider('Selecione o intervalo de percentual',
                            0.0, 100.0, (1.0, 100.0))
        st.write('Intervalo Selecionado:',intervalo)    

        # aplica o intervalo escolhido no slider
        df_aux = dfx[(dfx['perc_fator_nome'] >= intervalo[0]) & (dfx['perc_fator_nome'] < intervalo[1])]  
        perc_eliminados = \
            dfx[(dfx['perc_fator_nome'] < intervalo[0]) | (dfx['perc_fator_nome'] > intervalo[1])]['perc_fator_nome'].sum()

        # inclui uma linha do percentual eliminado
        df_inclui = {'fator_nome':'***** fora do intervalo', 'qtde_fator_nome': 0, 'perc_fator_nome': perc_eliminados}
        df_inclui = pd.DataFrame(df_inclui, index=([1]))
        df_aux = pd.concat([df_aux, df_inclui])              

        # passa os dados para o gráfico e exibe
        fig = px.pie( df_aux, values='perc_fator_nome', names='fator_nome', title='Fator Contribuinte')
        fig.update_layout(autosize=False)
        st.plotly_chart( fig, use_container_width=True )                                    

#-------- Visão Tática
with tab2:
    with st.container():
        # Percentual de Fatores por Área
        st.header( 'Percentual de Fatores por Área' )   

        # carrega o dataframe
        df_aux = seleciona_fator_area(df_fator_recomendacao)

        # passa os dados para o gráfico e exibe
        fig = px.pie( df_aux, values='perc_area', names='fator_area', title='Fator Área')
        fig.update_layout(autosize=False)
        st.plotly_chart( fig, use_container_width=True )          