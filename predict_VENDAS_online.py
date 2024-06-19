'''
Disponibiliza online o modelo de Machine Learning para fazer a predição da quantidade de 
vendas por produto e dia da semana por meio de modelo elaborado no projeto 
vendas_SUPERMERCADOpredict, para uso da equipe tática de vendas.

- Este projeto disponibiliza, através de um site, o modelo de machine learning criado e 
treinado no projeto vendas_SUPERMERCADOpredict, esse modelo permite fazer a predição da 
quantidade de vendas de um supermercado por produto e dia da semana.
'''

# importa as bibliotecas
import streamlit as st
from PIL import Image

# define o título que aparecerá na aba do navegador
st.set_page_config( page_title='predict_VENDASonline_i4x.Data', page_icon='')

# carrega imagem para o sidebar (barra lateral)
image_path = 'images/vendas_super.jpg'
image = Image.open( image_path )
st.sidebar.image( image, width=240 )

# define a cor de fundo do sidebar
st.markdown("""
<style>
    [data-testid=stSidebar] {
        background-color: #EAEAE8;
    }           
</style>
""", unsafe_allow_html=True)

# exibe frase no sidebar
st.sidebar.markdown('#### Powered by Jairo Bernardes Júnior')

# carrega imagem para o sidebar (barra lateral)
image_path = 'escopo_predict_VENDASonline.png'
image2 = Image.open( image_path )
st.image( image2, width=500 )

# exibe explicação no corpo da tela sobre o site vendas_SUPERMERCADOpredict_dash
st.markdown(
    """
    ### Disponibiliza o modelo de Machine Learning para fazer a predição da quantidade de vendas

    predict_VENDASonline foi construído para fazer predições de vendas de supermercado
    por produto e por dia da semana através de modelo de machine learning criado no projeto:
    vendas_SUPERMERCADOpredict.
    
    ### Contato
    - https://www.linkedin.com/in/jairobernardesjunior
    """)