import pandas as pd
import os

# Caminhos de entrada e saída
CAMINHO_ENTRADA = os.path.join(".silver", ".silver_aposentados", "step02_aposentados.parquet")
CAMINHO_SAIDA = os.path.join(".silver", ".silver_aposentados", "step03_aposentados.parquet")

def ajustar_dt_ing_serv_pub():
    # Carregar o arquivo Parquet da camada .silver
    df = pd.read_parquet(CAMINHO_ENTRADA)

    # Verificar qual coluna de ingresso está presente
    col_ente = None
    if "DT_ING_ENTE" in df.columns:
        col_ente = "DT_ING_ENTE"
    elif "DATA DE INGRESSO NO ENTE" in df.columns:
        col_ente = "DATA DE INGRESSO NO ENTE"
    else:
        raise KeyError("Nenhuma coluna de data de ingresso encontrada: 'DT_ING_ENTE' ou 'DATA DE INGRESSO NO ENTE'")

    # Garantir que os campos estejam no formato datetime
    df["DT_ING_SERV_PUB"] = pd.to_datetime(df["DT_ING_SERV_PUB"], errors='coerce', dayfirst=True)
    df["DT_NASC_APOSENTADO"] = pd.to_datetime(df["DT_NASC_APOSENTADO"], errors='coerce', dayfirst=True)
    df[col_ente] = pd.to_datetime(df[col_ente], errors='coerce', dayfirst=True)

    # Armazenar cópia original para comparação
    df["DT_ING_SERV_PUB_ORIG"] = df["DT_ING_SERV_PUB"]

    # Ajuste por idade < 18 anos
    def ajustar_idade_minima(dt_ing, dt_nasc, dt_ente):
        if pd.isnull(dt_ing) or pd.isnull(dt_nasc):
            return dt_ing
        idade = (dt_ing - dt_nasc).days // 365
        if idade < 18:
            return dt_ente
        return dt_ing

    df["DT_ING_SERV_PUB"] = df.apply(
        lambda row: ajustar_idade_minima(row["DT_ING_SERV_PUB"], row["DT_NASC_APOSENTADO"], row[col_ente]),
        axis=1
    )

    # Regra adicional: DT_ING_SERV_PUB não pode ser maior que DT_ING_ENTE
    df["DT_ING_SERV_PUB"] = df.apply(
        lambda row: min(row["DT_ING_SERV_PUB"], row[col_ente]) if pd.notnull(row["DT_ING_SERV_PUB"]) and pd.notnull(row[col_ente]) else row["DT_ING_SERV_PUB"],
        axis=1
    )

    # Identificar registros ajustados
    df_ajustados = df[df["DT_ING_SERV_PUB"] != df["DT_ING_SERV_PUB_ORIG"]]
    qtd_ajustes = len(df_ajustados)

    # Exibir os 10 primeiros registros ajustados no terminal
    print("\n🔍 Registros ajustados (até 10 linhas):")
    print(df_ajustados[[
        "ID_APOSENTADO_MATRICULA",
        col_ente,
        "DT_NASC_APOSENTADO",
        "DT_ING_SERV_PUB_ORIG",
        "DT_ING_SERV_PUB"
    ]].head(10).to_string(index=False))

    # Exibir contagem de alterações
    print("\n📊 Resumo das alterações:")
    print(f"→ DT_ING_SERV_PUB ajustado em {qtd_ajustes} registros")

    # Salvar o resultado ajustado
    df.drop(columns=["DT_ING_SERV_PUB_ORIG"], inplace=True)
    df.to_parquet(CAMINHO_SAIDA, index=False)

    # Contagem final
    total_original = len(df)
    total_ajustado = len(df)

    print(f"\n✅ Total de registros no arquivo original: {total_original}")
    print(f"✅ Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"📁 Arquivo salvo em: {CAMINHO_SAIDA}")

# Execução direta
if __name__ == "__main__":
    ajustar_dt_ing_serv_pub()