import pandas as pd
import os

# Caminhos de entrada e saída
CAMINHO_ENTRADA = os.path.join(".silver", ".silver_pensionistas", "step02_pensionistas.parquet")
CAMINHO_SAIDA = os.path.join(".silver", ".silver_pensionistas", "step03_pensionistas.parquet")

def classificar_fundo():
    # Carregar o arquivo Parquet da camada .silver
    df = pd.read_parquet(CAMINHO_ENTRADA)

    # Contar registros originais
    total_original = len(df)

    # Garantir que o campo esteja no formato datetime
    df["DT_NASC_INSTITUIDOR"] = pd.to_datetime(df["DT_NASC_INSTITUIDOR"], errors='coerce')

    # Data de corte conforme decreto
    data_corte_nascimento = pd.to_datetime("28/02/1957", dayfirst=True)

    # Contagem anterior
    contagem_antes = df["CO_TIPO_FUNDO"].value_counts().to_dict()

    # Aplicar a lógica de ajuste
    mask_ajuste = (df["DT_NASC_INSTITUIDOR"] < data_corte_nascimento) & (df["CO_TIPO_FUNDO"] == 2)
    df.loc[mask_ajuste, "CO_TIPO_FUNDO"] = 1  # Ajusta para FUNPREV

    # Contagem após os ajustes
    contagem_depois = df["CO_TIPO_FUNDO"].value_counts().to_dict()

    # Exibir contagens no terminal
    print("\nContagem por tipo de fundo:")
    print("Antes dos ajustes:")
    print(f"FUNPREV (1): {contagem_antes.get(1, 0)}")
    print(f"FUNFIN  (2): {contagem_antes.get(2, 0)}")

    print("\nApós os ajustes:")
    print(f"FUNPREV (1): {contagem_depois.get(1, 0)}")
    print(f"FUNFIN  (2): {contagem_depois.get(2, 0)}")

    # Salvar o resultado ajustado na camada .silver
    df.to_parquet(CAMINHO_SAIDA, index=False)

    # Verificação final
    total_ajustado = len(df)
    quantidade_ajustes = mask_ajuste.sum()
    print(f"\n🔧 Total de registros ajustados: {quantidade_ajustes}")
    print(f"\n✅ Total de registros no arquivo original: {total_original}")
    print(f"✅ Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"📁 Arquivo com classificação de fundos salvo em: {CAMINHO_SAIDA}")

# Execução direta
if __name__ == "__main__":
    classificar_fundo()
