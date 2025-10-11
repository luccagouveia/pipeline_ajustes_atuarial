import pandas as pd

# Constantes
SAL_MINIMO = 1518

# Caminhos dos arquivos
input_path = ".silver/.silver_servidor/servidor_step05.parquet"
output_path = ".silver/.silver_servidor/servidor_step06.parquet"

def ajustar_contribuicao():
    # Carregar o arquivo de entrada
    df = pd.read_parquet(input_path)

    # Contar registros originais
    total_original = len(df)

    # Calcular o valor correto da contribuição
    df["VL_CONTRIBUICAO_CALCULADA"] = df["VL_BASE_CALCULO"] * 0.14

    # Aplicar regra: se VL_BASE_CALCULO == SAL_MINIMO, então VL_CONTRIBUICAO = 0
    df.loc[df["VL_BASE_CALCULO"] == SAL_MINIMO, "VL_CONTRIBUICAO_CALCULADA"] = 0

    # Identificar registros que precisam ser ajustados
    ajustes = df["VL_CONTRIBUICAO"] != df["VL_CONTRIBUICAO_CALCULADA"]
    quantidade_ajustes = ajustes.sum()

    # Aplicar os ajustes
    df.loc[ajustes, "VL_CONTRIBUICAO"] = df.loc[ajustes, "VL_CONTRIBUICAO_CALCULADA"]

    # Remover a coluna auxiliar
    df.drop(columns=["VL_CONTRIBUICAO_CALCULADA"], inplace=True)

    # Salvar o resultado no arquivo de saída
    df.to_parquet(output_path, index=False)

    # Contar registros ajustados
    total_ajustado = len(df)

    # Imprimir a quantidade de registros ajustados
    print(f"Quantidade de registros ajustados em VL_CONTRIBUICAO: {quantidade_ajustes}")

    # Visualizar as 10 primeiras linhas com campos relevantes
    print("\nVisualização das 10 primeiras linhas após os ajustes:")
    print(df[["ID_SERVIDOR_MATRICULA", "VL_BASE_CALCULO", "VL_CONTRIBUICAO"]].head(10))

    # Verificação final
    print(f"\n✅ Total de registros no arquivo original: {total_original}")
    print(f"✅ Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"📁 Arquivo salvo em: {output_path}")

# Execução direta
if __name__ == "__main__":
    ajustar_contribuicao()