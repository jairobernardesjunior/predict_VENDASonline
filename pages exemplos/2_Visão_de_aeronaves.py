# Cria os insights sobre aeronaves em acidentes aeronáuticos

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
    df_aeronaves = pd.read_csv( 'dataset_analise/df_acidentes_analise_aero.csv' )
    df_aeronaves['ocorrencia_dia'] = pd.to_datetime(df_aeronaves['ocorrencia_dia'])
    return df_aeronaves

# monta o dataframe de fabricantes das aeronaves
@st.cache_data
def seleciona_fabricante(dfx):
    return dfx[['aeronave_fabricante', 'qtde_fabric', 'perc_fabric']].\
        drop_duplicates().sort_values('qtde_fabric', ascending=False)

# monta o dataframe de ano de fabricação das aeronaves
@st.cache_data
def seleciona_ano_fab(dfx):
    return dfx[['aeronave_ano_fabricacao', 'qtde_ano_fab', 'perc_ano_fab']].\
        drop_duplicates().sort_values('aeronave_ano_fabricacao', ascending=True)

# monta o dataframe de tipo de veículo por ano
@st.cache_data
def seleciona_tipo_veiculo_ano(dfx):
    dfx.ocorrencia_dia = pd.to_datetime(dfx.ocorrencia_dia)
    dfx['ano'] = dfx.ocorrencia_dia.dt.year
    dfx = dfx[['aeronave_tipo_veiculo', 'ano']]
    dfx['qtde_tipo'] = 0
    dfx = dfx.loc[:, :].groupby( ['ano', 'aeronave_tipo_veiculo']).count().reset_index()
    dfx = dfx.sort_values('ano', ascending=False)
    return dfx

# monta o dataframe de matrículas de aeronaves em ocorrências
@st.cache_data
def seleciona_matricula_ocorr(dfx):
    dfx = dfx[['aeronave_matricula']]
    dfx['qtde_matricula'] = 0

    dfx = dfx.loc[:, :].groupby( ['aeronave_matricula']).count().reset_index()
    dfx = dfx.sort_values('qtde_matricula', ascending=False)
    return dfx

#------------------- INÍCIO
df_aeronaves = le_arquivo_analise()

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
image_path = './images/aviao3.jpg'
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
        # Quantidade de Aeronaves por Fabricante - GBARRAS
        st.header( 'Quantidade de Aeronaves por Fabricante' )   

        # carrega o dataframe
        dfx = seleciona_fabricante(df_aeronaves)

        #-------- Controle de dados dupla face  - define intervalo online
        intervalo = st.slider('Selecione o intervalo de quantidade',
                            0.0, 100.0, (6.0, 100.0))
        st.write('Intervalo Selecionado:',intervalo)    

        # aplica o intervalo escolhido no slider
        df_aux = dfx[(dfx['qtde_fabric'] >= intervalo[0]) & (dfx['qtde_fabric'] < intervalo[1])]

        # passa os dados para o gráfico
        fig = px.bar ( df_aux, x='qtde_fabric', y='aeronave_fabricante', title='Aeronaves por Fabricante',                      
                       labels={
                            "qtde_fabric": "Quantidade",
                            "aeronave_fabricante": "Fabricante",
                              },                     
                     )
        
        # faz algumas configurações do gráfico
        fig.update_traces(width=0.8)
        fig.update_yaxes(tickfont=dict(size=8))
        fig.update_xaxes(tickfont=dict(size=8))
        fig.update_traces(marker_color='red')
        fig.update_layout(width=700, height=500, bargap=0.05)        

        # exibe o gráfico
        st.plotly_chart( fig, use_container_width=True)     

    with st.container():
        # Quantidade de Aeronaves por Ano de Fabricação - GBARRAS
        st.markdown( """___""")         
        st.header( 'Quantidade de Aeronaves por Ano de Fabricação' )   

        # carrega o dataframe e exclui alguns anos incompatíveis
        dfx = seleciona_ano_fab(df_aeronaves)
        dfx = dfx[(dfx['aeronave_ano_fabricacao'] > 0) & (dfx['aeronave_ano_fabricacao'] < 3000)]

        #-------- Controle de dados dupla face - define intervalo online
        min= dfx.loc[:, 'aeronave_ano_fabricacao'].min()/10
        max= dfx.loc[:, 'aeronave_ano_fabricacao'].max()/10
        min= min*10
        max= max*10

        intervalo = st.slider('Selecione o intervalo de ano',
                            min, max, (1980.0, max))
        st.write('Intervalo Selecionado:',intervalo)    

        # aplica o intervalo escolhido no slider
        df_aux = dfx[(dfx['aeronave_ano_fabricacao'] >= intervalo[0]) & (dfx['aeronave_ano_fabricacao'] < intervalo[1])]

        # passa os dados para o gráfico
        fig = px.bar ( df_aux, x='aeronave_ano_fabricacao', y='qtde_ano_fab', title='Aeronaves por Ano de Fabricação',                      
                       labels={
                            "qtde_ano_fab": "Quantidade",
                            "aeronave_ano_fabricacao": "Ano",
                              },                     
                     )
        
        # faz algumas configurações do gráfico
        fig.update_traces(width=0.8)
        fig.update_yaxes(tickfont=dict(size=8))
        fig.update_xaxes(tickfont=dict(size=8))
        fig.update_traces(marker_color='green')
        fig.update_layout(width=700, height=500, bargap=0.05)        

        # exibe o gráfico
        st.plotly_chart( fig, use_container_width=True)                                      

#-------- Visão Tática
with tab2:
    with st.container():
        # Quantidade de Aeronaves por Tipo de VSeículo e Ano - GBARRAS
        st.header( 'Quantidade de Tipo de Veículo Envolvidos por Ano' )   

        # carrega o dataframe
        dfx = seleciona_tipo_veiculo_ano(df_aeronaves)

        #-------- Controle de dados dupla face - define intervalo online
        min= dfx.loc[:, 'ano'].min()/10
        max= dfx.loc[:, 'ano'].max()/10
        min= min*10
        max= max*10

        intervalo = st.slider('Selecione o intervalo de ano',
                            min, max, (min, max))
        st.write('Intervalo Selecionado:',intervalo)    

        # aplica o intervalo escolhido no slider
        df_aux = dfx[(dfx['ano'] >= intervalo[0]) & (dfx['ano'] < intervalo[1])]

        # passa os dados para o gráfico
        fig = px.bar ( df_aux, x='ano', y='qtde_tipo', title='Tipo de Veículo Envolvidos por Ano',  
                       color='aeronave_tipo_veiculo', barmode='group',                    
                       labels={
                            "qtde_tipo": "Quantidade",
                            "ano": "Ano",
                            "aeronave_tipo_veiculo": "Tipo de Veículo",
                              },                     
                     )
        
        # faz algumas configurações do gráfico
        fig.update_traces(width=0.8)
        fig.update_yaxes(tickfont=dict(size=8))
        fig.update_xaxes(tickfont=dict(size=8))
        fig.update_layout(width=700, height=500, bargap=0.05)        

        # exibe o gráfico
        st.plotly_chart( fig, use_container_width=True) 

    with st.container():
        # Quantidade de Matrículas de Aeronaves em Ocorrências - GBARRA
        st.markdown( """___""")
        st.header( 'Quantidade de Matrículas de Aeronaves em Ocorrências' )   

        #-------- Controle de dados dupla face - define intervalo online
        intervalo = st.slider('Selecione o intervalo de quantidade',
                            0.0, 100.0, (10.0, 28.0))
        st.write('Intervalo Selecionado:',intervalo) 

        # carrega o dataframe aplica o intervalo escolhido no slider
        dfx = seleciona_matricula_ocorr(df_aeronaves)
        df_aux = dfx[(dfx['qtde_matricula'] >= intervalo[0]) & (dfx['qtde_matricula'] < intervalo[1])]
        perc_eliminados = \
            dfx[(dfx['qtde_matricula'] < intervalo[0]) | (dfx['qtde_matricula'] > intervalo[1])]['qtde_matricula'].sum()

        # passa os dados para o gráfico
        fig = px.bar ( df_aux, x='aeronave_matricula', y='qtde_matricula', title='Matrículas de Aeronaves em Ocorrências',                      
                       labels={
                            "qtde_matricula": "Quantidade",
                            "aeronave_matricula": "Matrícula",
                              },                     
                     )
        
        # faz algumas configurações do gráfico
        fig.update_traces(width=0.8)
        fig.update_yaxes(tickfont=dict(size=8))
        fig.update_xaxes(tickfont=dict(size=8))
        fig.update_traces(marker_color='blue')
        fig.update_layout(width=700, height=500, bargap=0.05)        

        # exibe o gráfico
        st.plotly_chart( fig, use_container_width=True)         