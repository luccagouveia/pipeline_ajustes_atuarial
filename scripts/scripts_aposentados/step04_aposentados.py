import pandas as pd
import os

# Caminhos de entrada e saída
CAMINHO_ENTRADA = os.path.join(".silver", ".silver_aposentados", "step03_aposentados.parquet")
CAMINHO_SAIDA = os.path.join(".silver", ".silver_aposentados", "step04_aposentados.parquet")

def classificar_fundo():
    # Carregar o arquivo Parquet da camada .silver
    df = pd.read_parquet(CAMINHO_ENTRADA)

    # Contar registros originais
    total_original = len(df)

    # Garantir que os campos estejam no formato datetime
    col_ente = "DT_ING_ENTE" if "DT_ING_ENTE" in df.columns else "DATA DE INGRESSO NO ENTE"
    df[col_ente] = pd.to_datetime(df[col_ente], errors='coerce', dayfirst=True)
    df["DT_NASC_APOSENTADO"] = pd.to_datetime(df["DT_NASC_APOSENTADO"], errors='coerce', dayfirst=True)

    # Datas de corte conforme os decretos
    data_corte_ingresso = pd.to_datetime("27/12/2018", dayfirst=True)
    data_corte_nascimento = pd.to_datetime("28/02/1957", dayfirst=True)

    # Contagem anterior (se já existir coluna de fundo)
    if "CO_TIPO_FUNDO" in df.columns:
        contagem_antes = df["CO_TIPO_FUNDO"].value_counts().to_dict()
    else:
        contagem_antes = {}

    # Função para aplicar a lógica de classificação
    def calcular_fundo(row):
        if pd.isnull(row[col_ente]) or pd.isnull(row["DT_NASC_APOSENTADO"]):
            return None
        if (
            row[col_ente] <= data_corte_ingresso and
            row["DT_NASC_APOSENTADO"] > data_corte_nascimento and
            str(row.get("IN_PREV_COMP", "")).strip() == "2"
        ):
            return 2  # FUNFIN
        return 1  # FUNPREV

    # Aplicar a nova classificação
    df["CO_TIPO_FUNDO_NOVO"] = df.apply(calcular_fundo, axis=1)

    # Contagem após os ajustes
    contagem_depois = df["CO_TIPO_FUNDO_NOVO"].value_counts().to_dict()

    # Calcular quantidade ajustada
    if "CO_TIPO_FUNDO" in df.columns:
        qtd_ajustada = (df["CO_TIPO_FUNDO"] != df["CO_TIPO_FUNDO_NOVO"]).sum()
    else:
        qtd_ajustada = len(df)

    # Atualizar coluna final
    df["CO_TIPO_FUNDO"] = df["CO_TIPO_FUNDO_NOVO"]
    df.drop(columns=["CO_TIPO_FUNDO_NOVO"], inplace=True)

    # Salvar o resultado ajustado
    df.to_parquet(CAMINHO_SAIDA, index=False)

    # Verificação final
    total_ajustado = len(df)

    # Exibir contagens no terminal
    print("\nContagem por tipo de fundo:")
    print("Antes dos ajustes:")
    print(f"FUNPREV (1): {contagem_antes.get(1, 0)}")
    print(f"FUNFIN  (2): {contagem_antes.get(2, 0)}")

    print("\nApós os ajustes:")
    print(f"FUNPREV (1): {contagem_depois.get(1, 0)}")
    print(f"FUNFIN  (2): {contagem_depois.get(2, 0)}")
    print(f"\nQuantidade ajustada: {qtd_ajustada}")

    print(f"\n✅ Total de registros no arquivo original: {total_original}")
    print(f"✅ Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"📁 Arquivo com classificação de fundos salvo em: {CAMINHO_SAIDA}")

# Execução direta
if __name__ == "__main__":
    classificar_fundo()