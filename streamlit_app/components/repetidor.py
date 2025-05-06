import streamlit as st

def criar_botao_home():
    if st.button("Home"):
        st.switch_page("pages/Home.py")