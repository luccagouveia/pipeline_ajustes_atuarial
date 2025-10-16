import pandas as pd
import os
import glob

# Criar pastas se não existirem
os.makedirs('./.silver/.silver_pensionistas', exist_ok=True)

# Buscar o arquivo mais recente que contenha "pensionistas_" no nome
arquivos_pensionistas = glob.glob('./.raw/pensionistas_*.xlsx')

def mais_recente(lista_arquivos):
    return max(lista_arquivos, key=os.path.getmtime) if lista_arquivos else None

arquivo_pensionistas = mais_recente(arquivos_pensionistas)

if not arquivo_pensionistas:
    raise FileNotFoundError("Nenhum arquivo com 'pensionistas_' encontrado na pasta .raw.")

# Leitura do arquivo Excel
df = pd.read_excel(arquivo_pensionistas, engine='openpyxl')

# Salvar os dados em formato Parquet na camada silver
df.to_parquet('./.silver/.silver_pensionistas/step01_pensionistas.parquet', index=False)

# Exibir resumo
print(f"Arquivo processado: {os.path.basename(arquivo_pensionistas)}")
print(f"Registros totais: {len(df)}")
print(f"Total de colunas: {len(df.columns)}")
print(f"Nomes das colunas: {df.columns.tolist()}")