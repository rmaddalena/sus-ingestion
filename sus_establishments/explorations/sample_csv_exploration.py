# Databricks notebook source
import os
import sys
import pandas as pd
import csv

sys.path.append("/Workspace/Users/rafaelmaddalena@gmail.com/sus_ingestion")

file_path = '/Workspace/Users/rafaelmaddalena@gmail.com/raw/STSP2501.csv'

# COMMAND ----------

# 1. Descrição dos dados

# Se houver erro de separador, adicione sep=';' ou sep=','
df = pd.read_csv(file_path, encoding='mac_roman')

# Ver as primeiras 5 linhas
print(df.head())

# Ver informações sobre tipos de dados e valores nulos
# Isso também mostra o uso de memória do DataFrame no Python
print(df.info())

# Estatísticas descritivas (média, min, max, etc. para colunas numéricas)
print(df.describe())

# Verificar se existem valores faltantes por coluna
print(df.isnull().sum())

# Ver os nomes de todas as colunas
print(df.columns.tolist())

# COMMAND ----------

# 2. Descrição do arquivo

# Tamanho do arquivo em Bytes (disco)
file_size_bytes = os.path.getsize(file_path)
file_size_mb = file_size_bytes / (1024 * 1024)

# Carregar os dados (ajuste o encoding/sep se necessário)
df = pd.read_csv(file_path, sep=',', encoding='MacRoman')

# Obter Linhas e Colunas
linhas, colunas = df.shape

print(f"--- Estatísticas do Arquivo ---")
print(f"Tamanho em disco: {file_size_bytes} bytes ({file_size_mb:.2f} MB)")
print(f"Total de Linhas: {linhas}")
print(f"Total de Colunas: {colunas}")
