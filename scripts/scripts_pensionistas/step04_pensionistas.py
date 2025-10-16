import pandas as pd
import os

# Caminho de entrada e saída
CAMINHO_ENTRADA = os.path.join(".silver", ".silver_pensionistas", "step03_pensionistas.parquet")
CAMINHO_SAIDA = os.path.join(".silver", ".silver_pensionistas", "step04_pensionistas.parquet")

# Verificar se o arquivo existe
if not os.path.exists(CAMINHO_ENTRADA):
    raise FileNotFoundError(f"Arquivo não encontrado: {CAMINHO_ENTRADA}")

# Carregar os dados
df = pd.read_parquet(CAMINHO_ENTRADA)

# Contagem inicial
total_entrada = len(df)

# Copiar para ajuste
df_ajustado = df.copy()

# Identificar registros com VL_PCT_QUOTA vazio ou nulo
mask_quota_vazia = df_ajustado['VL_PCT_QUOTA'].isna()

# Preencher VL_PCT_QUOTA conforme regra
for instituidor, grupo in df_ajustado[mask_quota_vazia].groupby('ID_INSTITUIDOR_MATRICULA'):
    qtd = len(grupo)
    valor_quota = 100 if qtd == 1 else round(100 / qtd, 2)
    df_ajustado.loc[grupo.index, 'VL_PCT_QUOTA'] = valor_quota

# Calcular VL_TOT_PENSAO com base na nova VL_PCT_QUOTA
df_ajustado['VL_TOT_PENSAO_CALCULADO'] = df_ajustado['VL_BENEF_PENSAO'] / (df_ajustado['VL_PCT_QUOTA'] / 100)

# Aplicar regras de limite
df_ajustado['VL_TOT_PENSAO_AJUSTADO'] = df_ajustado[['VL_TOT_PENSAO_CALCULADO', 'VL_TETO_ESPECIFICO']].min(axis=1)
df_ajustado['VL_TOT_PENSAO_AJUSTADO'] = df_ajustado['VL_TOT_PENSAO_AJUSTADO'].apply(lambda x: max(x, 1518))

# Verificar registros ajustados
mask_ajuste = df_ajustado['VL_TOT_PENSAO_AJUSTADO'] != df_ajustado['VL_TOT_PENSAO']
ajustes_realizados = mask_ajuste.sum()

# Exibir os 10 primeiros registros ajustados
df_verificacao = df_ajustado.loc[mask_ajuste, [
    'ID_INSTITUIDOR_MATRICULA',
    'ID_PENSIONISTA_MATRICULA',
    'VL_BENEF_PENSAO',
    'VL_TOT_PENSAO',
    'VL_TOT_PENSAO_AJUSTADO'
]]

print("🔍 Registros ajustados (até 10 linhas):")
print(df_verificacao.head(10))

# Substituir VL_TOT_PENSAO pelos valores ajustados e salvar
df_ajustado['VL_TOT_PENSAO'] = df_ajustado['VL_TOT_PENSAO_AJUSTADO']
df_ajustado.drop(columns=['VL_TOT_PENSAO_CALCULADO', 'VL_TOT_PENSAO_AJUSTADO'], inplace=True)
df_ajustado.to_parquet(CAMINHO_SAIDA, index=False)

# Contagem final
total_saida = len(df_ajustado)

# Exibir resumo
print(f"\nTotal de registros de entrada: {total_entrada}")
print(f"Total de registros de saída: {total_saida}")
print(f"Total de instituidores únicos: {df['ID_INSTITUIDOR_MATRICULA'].nunique()}")
print(f"Registros ajustados em VL_TOT_PENSAO: {ajustes_realizados}")
print(f"Arquivo salvo em: {CAMINHO_SAIDA}")