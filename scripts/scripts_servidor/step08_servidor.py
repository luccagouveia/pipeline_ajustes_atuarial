import pandas as pd
import os

# Caminho baseado no diretório atual do script
base_dir = os.path.dirname(os.path.abspath(__file__))
input_parquet_path = os.path.join(base_dir, "..", "..", ".silver", ".silver_servidor", "servidor_step07.parquet")
output_excel_path = os.path.join(base_dir, "..", "..", "gold", "servidor_final.xlsx")

def exportar_para_excel():
    # Ler o arquivo Parquet
    df = pd.read_parquet(input_parquet_path)

    # Contar registros originais
    total_original = len(df)

    # Remover colunas auxiliares
    df = df.drop(columns=["IDADE_ORIGINAL", "IDADE_AJUSTADA"], errors='ignore')

    # Exportar para Excel
    df.to_excel(output_excel_path, index=False, engine='openpyxl')

    # Ler o arquivo exportado para verificação
    df_exportado = pd.read_excel(output_excel_path, engine='openpyxl')
    total_exportado = len(df_exportado)
    colunas_exportadas = df_exportado.columns.tolist()
    total_colunas = len(colunas_exportadas)

    # Exibir resultados
    print(f"✅ Total de registros no arquivo original: {total_original}")
    print(f"✅ Total de registros no arquivo exportado: {total_exportado}")
    print(f"📊 Total de colunas exportadas: {total_colunas}")
    print(f"📝 Colunas exportadas: {colunas_exportadas}")
    print(f"📁 Arquivo exportado com sucesso para: {output_excel_path}")

# Execução direta
if __name__ == "__main__":
    exportar_para_excel()