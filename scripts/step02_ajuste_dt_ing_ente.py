import pandas as pd
import os

def executar():
    # Caminhos de entrada e saída
    caminho_entrada = './.silver/servidor_tratado.parquet'
    caminho_saida = './.silver/servidores_ajuste_dt_ing_ente.parquet'

    # Verificar se o arquivo de entrada existe
    if not os.path.exists(caminho_entrada):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_entrada}")

    print(f"Processando arquivo: {os.path.basename(caminho_entrada)}")

    # Carregar o arquivo original
    df_original = pd.read_parquet(caminho_entrada)

    # Converter datas para datetime
    df_original['DT_ING_ENTE'] = pd.to_datetime(df_original['DT_ING_ENTE'], errors='coerce')
    df_original['DT_NASC_SERVIDOR'] = pd.to_datetime(df_original['DT_NASC_SERVIDOR'], errors='coerce')

    # Calcular idade original
    df_original['IDADE_ORIGINAL'] = (df_original['DT_ING_ENTE'] - df_original['DT_NASC_SERVIDOR']).dt.days // 365

    # Criar cópia para ajuste
    df_ajustado = df_original.copy()

    # Identificar registros com idade menor que 18
    mask_idade_menor_que_18 = df_ajustado['IDADE_ORIGINAL'] < 18

    # Aplicar ajuste: DT_NASC_SERVIDOR = DT_ING_ENTE - 25 anos
    df_ajustado.loc[mask_idade_menor_que_18, 'DT_NASC_SERVIDOR'] = df_ajustado.loc[mask_idade_menor_que_18, 'DT_ING_ENTE'] - pd.DateOffset(years=25)

    # Salvar o resultado ajustado
    df_ajustado.drop(columns=['IDADE_ORIGINAL'], inplace=True)
    df_ajustado.to_parquet(caminho_saida, index=False)

    print(f"Registros ajustados: {mask_idade_menor_que_18.sum()}")
    print(f"Arquivo salvo em: {caminho_saida}")

    # Verificação: mostrar apenas registros que foram ajustados
    df_verificacao = pd.DataFrame({
        'DT_ING_ENTE': df_original.loc[mask_idade_menor_que_18, 'DT_ING_ENTE'],
        'DT_NASC_ORIGINAL': df_original.loc[mask_idade_menor_que_18, 'DT_NASC_SERVIDOR'],
        'IDADE_ORIGINAL': df_original.loc[mask_idade_menor_que_18, 'IDADE_ORIGINAL'],
        'DT_NASC_AJUSTADO': df_ajustado.loc[mask_idade_menor_que_18, 'DT_NASC_SERVIDOR'],
        'IDADE_AJUSTADA': 25
    })

    print("\n🔍 Registros ajustados (até 10 linhas):")
    print(df_verificacao.head(10))

# Executar se chamado diretamente
if __name__ == "__main__":
    executar()