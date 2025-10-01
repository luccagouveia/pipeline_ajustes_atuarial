import pandas as pd
import os
import glob

# Criar pastas se não existirem
os.makedirs('./gold', exist_ok=True)
os.makedirs('./.silver', exist_ok=True)

# Buscar o arquivo mais recente que contenha "servidor_" no nome
arquivos_servidor = glob.glob('./.raw/servidor_*.xlsx')

def mais_recente(lista_arquivos):
    return max(lista_arquivos, key=os.path.getmtime) if lista_arquivos else None

arquivo_servidor = mais_recente(arquivos_servidor)

if not arquivo_servidor:
    raise FileNotFoundError("Nenhum arquivo com 'servidor_' encontrado na pasta .raw.")

# Leitura do arquivo Excel
df = pd.read_excel(arquivo_servidor, engine='openpyxl')

# Filtrar registros de servidores comissionados sem contribuição e vínculo tipo 4
filtro_comissionados = (
    df['VL_BASE_CALCULO'].isna() &
    df['VL_CONTRIBUICAO'].isna() &
    (df['CO_TIPO_VINCULO'] == 4)
)

# Criar nova tabela com os registros comissionados
df_comissionados = df[filtro_comissionados]

# Salvar os registros comissionados em Excel na camada gold
df_comissionados.to_excel('./gold/servidor_comissionados.xlsx', index=False)

# Remover esses registros da base original
df_restante = df[~filtro_comissionados]

# Salvar o restante dos dados em formato Parquet na camada silver
df_restante.to_parquet('./.silver/servidor_tratado.parquet', index=False)

# Exibir resumo
print(f"Arquivo processado: {os.path.basename(arquivo_servidor)}")
print(f"Registros totais: {len(df)}")
print(f"Registros comissionados extraídos: {len(df_comissionados)}")
print(f"Registros restantes salvos em Parquet: {len(df_restante)}")
