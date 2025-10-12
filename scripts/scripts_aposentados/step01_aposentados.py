import pandas as pd
import os
import glob

# Criar pastas se não existirem
os.makedirs('./.silver/.silver_aposentados', exist_ok=True)

# Buscar o arquivo mais recente que contenha "aposentados_" no nome
arquivos_aposentados = glob.glob('./.raw/aposentados_*.xlsx')

def mais_recente(lista_arquivos):
    return max(lista_arquivos, key=os.path.getmtime) if lista_arquivos else None

arquivo_aposentados = mais_recente(arquivos_aposentados)

if not arquivo_aposentados:
    raise FileNotFoundError("Nenhum arquivo com 'aposentados_' encontrado na pasta .raw.")

# Leitura do arquivo Excel
df = pd.read_excel(arquivo_aposentados, engine='openpyxl')

# Salvar os dados em formato Parquet na camada silver
df.to_parquet('./.silver/.silver_aposentados/step01_aposentados.parquet', index=False)

# Exibir resumo
print(f"Arquivo processado: {os.path.basename(arquivo_aposentados)}")
print(f"Registros totais: {len(df)}")
print(f"Total de colunas: {len(df.columns)}")
print(f"Nomes das colunas: {df.columns.tolist()}")