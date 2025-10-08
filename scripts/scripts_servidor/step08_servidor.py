import pandas as pd
import os

# Caminho baseado no diretório atual do script
base_dir = os.path.dirname(os.path.abspath(__file__))
input_parquet_path = os.path.join(base_dir, "..", "..", ".silver", ".silver_servidor", "servidor_step07.parquet")
output_excel_path = os.path.join(base_dir, "..", "..", "gold", "servidor_final.xlsx")

# Ler e exportar
df = pd.read_parquet(input_parquet_path)
df.to_excel(output_excel_path, index=False, engine='openpyxl')

print(f"Arquivo exportado com sucesso para: {output_excel_path}")
