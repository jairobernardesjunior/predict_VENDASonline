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
def le_arquivo_analise():
    df_ocorrencias = pd.read_csv( 'dataset_analise/df_acidentes_analise_ocorr.csv' )
    df_ocorrencias['ocorrencia_dia'] = pd.to_datetime(df_ocorrencias['ocorrencia_dia'])
    return df_ocorrencias

# monta dataframe de latitude e longitude e faz alguns ajustes
@st.cache_data
def seleciona_latlong(dfx):
    dfx = df_ocorrencias[['ocorrencia_cidade', 'ocorrencia_uf', 'ocorrencia_aerodromo', \
                            'ocorrencia_latitude', 'ocorrencia_longitude', 'qtde_ocorr_aerod', \
                            'qtde_aeron' \
                        ]].drop_duplicates().sort_values('ocorrencia_cidade', ascending=True)
    
    lati_brasil = -4.276389
    latf_brasil = -33.57389
    longi_brasil = -74.38611
    longf_brasil = -34.05639

    dfx = dfx.loc[(dfx['ocorrencia_latitude'] != 0) & \
                  (dfx['ocorrencia_longitude'] != 0) & \
                  (dfx['ocorrencia_aerodromo'] != '***') & \
                  (dfx['ocorrencia_aerodromo'] != '****') & \
                  
                  (dfx['ocorrencia_uf'] != 'AM') & \
                  (dfx['ocorrencia_uf'] != 'RR') & \
                  (dfx['ocorrencia_uf'] != 'PB') & \
                  (dfx['ocorrencia_uf'] != 'PA') & \
                  
                  (dfx['ocorrencia_latitude'] < lati_brasil) & \
                  (dfx['ocorrencia_latitude'] > latf_brasil) & \
                  
                  (dfx['ocorrencia_longitude'] > longi_brasil) & \
                  (dfx['ocorrencia_longitude'] < longf_brasil) \

                   , :]
    
    dfx.columns = ['cidade', 'UF', 'aeródromo', 'lat', 'long', 'ocorrências', 'aeronaves']
    return dfx

# monta o dataframe de tipo de ocorrências
@st.cache_data
def seleciona_tipo_ocorrencia(dfx):
    return dfx[['ocorrencia_tipo', 'qtde_tipo_ocorr', 'perc_tipo_ocorr']].\
        drop_duplicates().sort_values('perc_tipo_ocorr', ascending=False)

# monta o dataframe de classificação do acidente por UF
@st.cache_data
def agrupa_uf_classificacao(dfx):
    # colunas
    cols = ['ocorrencia_uf', 'ocorrencia_classificacao', 'qtde_classif']

    # selecao de linhas
    dfx = df_ocorrencias[['ocorrencia_uf', 'ocorrencia_classificacao']]
    dfx['qtde_classif'] = 0

    dfx = dfx.loc[:, cols].groupby( ['ocorrencia_uf', 'ocorrencia_classificacao']).count().reset_index()
    return dfx.sort_values(['ocorrencia_uf'], ascending=False)    

# monta o dataframe de saidas da pista por cidade e aeródromo
@st.cache_data
def seleciona_saida_pista_aerodromo(dfx):
    dfx = dfx[['ocorrencia_cidade', 'ocorrencia_aerodromo', 'qtde_saip_aerod', 'qtde_saip_total']].\
        sort_values('qtde_saip_aerod', ascending=False)
    dfx = dfx.drop_duplicates().dropna()
    dfx = dfx[(dfx['qtde_saip_aerod'] > 0) & 
              (dfx['ocorrencia_aerodromo'] != '***') &
              (dfx['ocorrencia_aerodromo'] != '****') &
              (dfx['ocorrencia_aerodromo'] != '**NI')]
    dfx['cidade_aerodromo'] = dfx['ocorrencia_cidade'] + ' - ' + dfx['ocorrencia_aerodromo']

    return dfx

# monta o dataframe de ocorrências por aeródromo
@st.cache_data
def seleciona_aerodromo_ocorr(dfx):
    dfx = dfx[['ocorrencia_aerodromo', 'qtde_ocorr_aerod']].\
        sort_values('qtde_ocorr_aerod', ascending=False)

    return dfx.drop_duplicates().dropna()

#------------------- INÍCIO
df_ocorrencias = le_arquivo_analise()

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
image_path = './images/aviao2.jpg'
ix = Image.open( image_path ) 
st.sidebar.image( ix, width=240 )

#-------- Empresa
st.sidebar.markdown('###### Powered by Jairo Bernardes Júnior')
#st.sidebar.markdown( '##### **Site em constante evolução')

#----------------------------------------------
# gráficos
#----------------------------------------------
#-------- Abas de finalidade dos dados
tab1, tab2, tab3 = st.tabs( ['Visão Estratégica', 'Visão Tática', 'Visão Geográfica'])

#-------- Visão Estratégica
with tab1:
    with st.container():
        # Classificação de ocorrências por UF - GBARRAS GRUPOS
        st.header( 'Classificação de Ocorrências por UF' )

        dfx = agrupa_uf_classificacao(df_ocorrencias)

        # desenhar o gráfico de colunas
        # Plotly  
        fig = px.bar(dfx, x="qtde_classif", y="ocorrencia_uf", color="ocorrencia_classificacao",
                     title= 'Classificação/UF',
                     orientation='h',
                     labels={
                            "qtde_classif": "Ocorrências",
                            "ocorrencia_uf": "UF",
                            "ocorrencia_classificacao": "Classificação"
                            },                     
                     )
        
        # faz algumas configurações do gráfico
        fig.update_traces(width=0.8)
        fig.update_yaxes(tickfont=dict(size=8))
        fig.update_xaxes(tickfont=dict(size=8))
        fig.update_layout(width=700, height=550, bargap=0.05)        

        # exibe o gráfico
        st.plotly_chart( fig, use_container_width=True)

    with st.container():
        # Percentual de Tipos de ocorrência - GPIZZA
        st.markdown( """___""")
        st.header( 'Percentual de Tipos de Ocorrência' )   

        #-------- Controle de dados dupla face - define intervalo online
        intervalo = st.slider('Selecione o intervalo de percentual',
                            0.0, 100.0, (1.75, 100.0))
        st.write('Intervalo Selecionado:',intervalo) 

        # carrega o dataframe aplica o intervalo escolhido no slider e calcula a diferença do percentual
        # eliminado
        dfx = seleciona_tipo_ocorrencia(df_ocorrencias)
        df_aux = dfx[(dfx['perc_tipo_ocorr'] >= intervalo[0]) & (dfx['perc_tipo_ocorr'] < intervalo[1])]
        perc_eliminados = \
            dfx[(dfx['perc_tipo_ocorr'] < intervalo[0]) | (dfx['perc_tipo_ocorr'] > intervalo[1])]['perc_tipo_ocorr'].sum()

        # inclui uma linha do percentual eliminado
        df_inclui = {'ocorrencia_tipo':'***** fora do intervalo', 'qtde_tipo_ocorr': 0, 'perc_tipo_ocorr': perc_eliminados}
        df_inclui = pd.DataFrame(df_inclui, index=([1]))
        df_aux = pd.concat([df_aux, df_inclui])

        # passa os dados para o gráfico de pizza e exibe
        fig = px.pie( df_aux, values='perc_tipo_ocorr', names='ocorrencia_tipo', title='Tipos de Ocorrências')
        fig.update_layout(autosize=False)
        st.plotly_chart( fig, use_container_width=True )                                           

#-------- Visão Tática
with tab2:
    with st.container():
        # Quantidade de Saidas da Pista por Aeródromo - GBARRAS
        st.header( 'Quantidade de Saidas da Pista por Aeródromo' )   

        # carrega o dataframe
        dfx = seleciona_saida_pista_aerodromo(df_ocorrencias)

        #-------- Controle de dados dupla face - define intervalo online
        intervalo = st.slider('Selecione o intervalo de quantidade',
                            0.0, 100.0, (6.0, 100.0))
        st.write('Intervalo Selecionado:',intervalo)    

        # aplica o intervalo escolhido no slider
        df_aux = dfx[(dfx['qtde_saip_aerod'] >= intervalo[0]) & (dfx['qtde_saip_aerod'] < intervalo[1])]

        # passa os dados para o gráfico
        fig = px.bar ( df_aux, x='qtde_saip_aerod', y='cidade_aerodromo',    
                       title= 'Saidas da Pista',                  
                       labels={
                            "qtde_saip_aerod": "Quantidade",
                            "cidade_aerodromo": "Aeródromo",
                              },                     
                     )
        
        # faz algumas configurações do gráfico
        fig.update_traces(width=0.8)
        fig.update_yaxes(tickfont=dict(size=8))
        fig.update_xaxes(tickfont=dict(size=8))
        fig.update_traces(marker_color='blue')
        fig.update_layout(width=700, height=600, bargap=0.05)

        # exibe o gráfico
        st.plotly_chart( fig, use_container_width=True)     

    with st.container():
        # Quantidade de Ocorrências por Aeródromo - GBARRAS
        st.markdown( """___""")        
        st.header( 'Quantidade de Ocorrências por Aeródromo' )   

        # carrega o dataframe
        dfx = seleciona_aerodromo_ocorr(df_ocorrencias)

        #-------- Controle de dados dupla face  - define intervalo online
        intervalo = st.slider('Selecione o intervalo de quantidade',
                            0.0, 100.0, (12.0, 100.0))
        st.write('Intervalo Selecionado:',intervalo)    

        # aplica o intervalo escolhido no slider
        df_aux = dfx[(dfx['qtde_ocorr_aerod'] >= intervalo[0]) & (dfx['qtde_ocorr_aerod'] < intervalo[1])]

        # passa os dados para o gráfico
        fig = px.bar ( df_aux, x='qtde_ocorr_aerod', y='ocorrencia_aerodromo', title='Ocorrências por Aeródromo',                      
                       labels={
                            "qtde_ocorr_aerod": "Quantidade",
                            "ocorrencia_aerodromo": "Aeródromo",
                              },                     
                     )
        
        # faz algumas configurações do gráfico
        fig.update_traces(width=0.8)
        fig.update_yaxes(tickfont=dict(size=8))
        fig.update_xaxes(tickfont=dict(size=8))
        fig.update_traces(marker_color='orange')
        fig.update_layout(width=700, height=500, bargap=0.05)        

        # exibe o gráfico
        st.plotly_chart( fig, use_container_width=True)                

#-------- Visão Geográfica
with tab3:
    # Localização das ocorrências aeronáuticas - GMAP
    st.header( 'Localização das Ocorrências Aeronáuticas' )   

    # carrega o dataframe
    df_aux = seleciona_latlong(df_ocorrencias)

    # define uma variável map do folium.Map() 
    map = folium.Map()  

    # passa os dados para o map
    for index, location_info in df_aux.iterrows():
        folium.Marker( [location_info['lat'],
                        location_info['long']],
                        popup=location_info[[
                        'cidade', 'UF', 'aeródromo', 'ocorrências', \
                        'aeronaves']] ).add_to( map )
    
    # exibe o mapa com as coordenadas e alguns dados de ocorrências
    folium_static( map, width=1024, height=600 )