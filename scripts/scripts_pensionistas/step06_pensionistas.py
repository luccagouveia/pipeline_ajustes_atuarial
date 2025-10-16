import pandas as pd
import os
from datetime import datetime

# Caminhos de entrada e saída
CAMINHO_ENTRADA = os.path.join(".silver", ".silver_pensionistas", "step05_pensionistas.parquet")
CAMINHO_SAIDA = os.path.join(".silver", ".silver_pensionistas", "step06_pensionistas.parquet")

# Verificar se o arquivo existe
if not os.path.exists(CAMINHO_ENTRADA):
    raise FileNotFoundError("Arquivo de entrada não encontrado.")

# Carregar os dados
df = pd.read_parquet(CAMINHO_ENTRADA)

# Contagem inicial
total_entrada = len(df)

# Data-base para cálculo da idade
data_base = datetime.strptime("01/09/2025", "%d/%m/%Y")

# === AJUSTE 1: CO_TIPO_RELACAO ===
qtde_relacao_antes = df['CO_TIPO_RELACAO'].isna().sum()
df['CO_TIPO_RELACAO'] = df['CO_TIPO_RELACAO'].fillna(6)
qtde_relacao_depois = df['CO_TIPO_RELACAO'].isna().sum()

# === AJUSTE 2: CO_CONDICAO ===
df['CO_CONDICAO'] = df['CO_TIPO_RELACAO'].apply(lambda x: 2 if x == 3 else 1)
qtde_condicao_2 = (df['CO_CONDICAO'] == 2).sum()
qtde_condicao_1 = (df['CO_CONDICAO'] == 1).sum()

# === AJUSTE 3: CO_DURACAO ===
df['DT_NASC_PENSIONISTA'] = pd.to_datetime(df['DT_NASC_PENSIONISTA'], format="%d/%m/%Y", errors='coerce')
df['idade_pensionista'] = df['DT_NASC_PENSIONISTA'].apply(
    lambda x: data_base.year - x.year - ((data_base.month, data_base.day) < (x.month, x.day))
    if pd.notnull(x) else None
)
df['CO_DURACAO'] = df['idade_pensionista'].apply(lambda x: 2 if x is not None and x < 21 else 1)
qtde_duracao_2 = (df['CO_DURACAO'] == 2).sum()
qtde_duracao_1 = (df['CO_DURACAO'] == 1).sum()

# === AJUSTE 4: NU_TEMPO_DURACAO ===
df['NU_TEMPO_DURACAO'] = df.apply(
    lambda row: max(21 - row['idade_pensionista'], 0) if row['CO_DURACAO'] == 2 and row['idade_pensionista'] is not None else None,
    axis=1
)
qtde_tempo_duracao = df['NU_TEMPO_DURACAO'].notna().sum()

# Salvar resultado final
df.to_parquet(CAMINHO_SAIDA, index=False)

# Exibir resumo
print("🔧 AJUSTES REALIZADOS")
print(f"1. CO_TIPO_RELACAO preenchido com 6 em {qtde_relacao_antes} registros nulos.")
print(f"2. CO_CONDICAO: {qtde_condicao_2} registros como '2 - Inválido', {qtde_condicao_1} como '1 - Válido'.")
print(f"3. CO_DURACAO: {qtde_duracao_2} registros como '2 - Temporário', {qtde_duracao_1} como '1 - Vitalício'.")
print(f"4. NU_TEMPO_DURACAO calculado para {qtde_tempo_duracao} registros com duração temporária.")
