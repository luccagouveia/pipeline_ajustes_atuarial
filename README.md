# 📊 Pipeline_ajustes_atuarial

## Projeto voltado para o tratamento, ajuste e qualificação de bases atuariais do RPPS do Município de São Paulo, com foco em servidores ativos, aposentados e pensionistas. O pipeline realiza validações, correções e ajustes conforme regras de negócio e legislação vigente, visando entregar produtos finais consistentes e auditáveis.

### 🗂️ Estrutura de Diretórios
        pipeline_ajustes_atuarial/
        ├── .raw/                  # Dados originais (.xlsx)
        ├── .silver/               # Dados intermediários tratados (Parquet)
        │   └── .silver_servidor/
        ├── gold/                  # Dados finais ajustados (.xlsx)
        ├── iconfig/               # Arquivos de configuração (YAML/JSON)
        ├── logs/                  # Logs de execução e auditoria
        ├── scripts/               # Scripts de tratamento por tipo de base
        │   └── scripts_servidor/
        ├── venv/                  # Ambiente virtual Python
        ├── integrador.py          # Script que executa todos os steps
        ├── README.md              # Documentação do projeto
        └── requirements.txt       # Dependências do projeto


## ⚙️ Etapas do Pipeline

    step01_servidor.py
        Segrega registros de servidores comissionados sem contribuição e vínculo tipo 4. Esses registros são exportados diretamente para a camada .gold, enquanto os demais seguem para a camada .silver.

    step02_servidor.py
        Ajusta o campo DT_NASC_SERVIDOR para garantir idade mínima de 25 anos no ingresso (DT_ING_ENTE). Exibe no terminal os registros alterados, comparando valores originais e ajustados.

    step03_servidor.py
        Normaliza os campos DT_ING_SERV_PUB, DT_ING_CARREIRA e DT_ING_CARGO com base em DT_NASC_SERVIDOR, assegurando idade mínima de 18 anos. Se inferior, os campos são ajustados para DT_ING_ENTE. Garante também que DT_ING_SERV_PUB não seja posterior a DT_ING_ENTE. Exibe os 10 primeiros registros ajustados e estatísticas de alterações por campo.


    step04_servidor.py
        Classifica os servidores entre os fundos FUNPREV (1) e FUNFIN (2) conforme os Decretos Municipais nº 61.151/2022 e nº 64.144/2025.

        FUNFIN (2): Admitidos até 27/12/2018, nascidos após 28/02/1957, e não aderentes à previdência complementar (IN_PREV_COMP == "2").
        FUNPREV (1): Demais casos, incluindo os que aderiram ao RPC (IN_PREV_COMP == "1").
        O resultado é salvo na camada .silver, com contagem por tipo de fundo exibida no terminal.


    step05_servidor.py
        Ajusta os campos VL_BASE_CALCULO e VL_REMUNERACAO conforme critérios mínimos e máximos:

        Não podem ser nulos.
        Devem ser ≥ R$1.518 (salário mínimo).
        Devem ser ≤ VL_TETO_ESPECIFICO.

    step06_servidor.py
        Recalcula VL_CONTRIBUICAO como 14% de VL_BASE_CALCULO.

        Se VL_BASE_CALCULO == SAL_MINIMO, então VL_CONTRIBUICAO = 0.

    step07_servidor.py
        Realiza ajustes complementares:

        Preenche CO_CRITERIO_ELEGIBILIDADE com 1 se estiver vazio.
        Ajusta CO_PODER e CO_TIPO_PODER com base em NO_ORGAO.
        Limita NU_TEMPO_RGPS ao teto atuarial de 22.280 meses.

    step08_servidor.py
        Exporta o resultado final da base de servidores da camada .silver para .xlsx na camada .gold, consolidando os dados tratados.
    
    integrador.py
        Executa todos os steps sequencialmente, com registro de logs via loguru.

## ⚙️ Tecnologias Utilizadas

    Python 3.10+
    Pandas e PyArrow (manipulação de dados e Parquet)
    OpenPyXL (leitura/escrita de arquivos Excel)
    pyarrow
    Dateutil (manipulação de datas)
    VSCode (ambiente de desenvolvimento)


## 📅 Plano de Desenvolvimento – Etapas do Projeto

    ✅ Fase 1 – Estruturação do Ambiente

        Criação da estrutura de diretórios por camada (.raw, .silver, .gold) e por tipo de base.
        Organização modular dos scripts por etapa (stepXX) e por categoria (scripts_servidor, etc.).

    ✅ Fase 2 – Criação do Ambiente Virtual

        Criação do ambiente com venv.
        Instalação dos pacotes essenciais: pandas, openpyxl, pyarrow, loguru, entre outros.

    ✅ Fase 3 – Implementação dos Scripts de Tratamento

        Desenvolvimento dos scripts step01 a step08 para servidores ativos.
        Aplicação das regras de negócio e ajustes conforme relatório de críticas.
        Salvamento dos resultados intermediários em .silver e finais em .gold.

    ✅ Fase 4 – Integração e Automação

        Criação do script integrador.py para execução sequencial dos steps.
        Registro de logs de execução com loguru em logs/integrador.log.

    🔄 Fase 5 – Validação dos Resultados

        Verificação da consistência dos dados ajustados.
        Análise do arquivo servidor_final.xlsx para entender por que mantém o mesmo número de linhas do original (116.749).
        Identificação de possíveis falhas na lógica de exclusão ou filtragem.

    🔄 Fase 6 – Expansão para Aposentados e Pensionistas

        Criação das estruturas e scripts específicos para aposentados e pensionistas.
        Adaptação das regras de negócio conforme cada tipo de base.

    🔄 Fase 7 – Documentação e Testes Finais

        Atualização do README.md com todas as fases e regras aplicadas.
        Criação de notebooks de teste (se necessário).
        Validação da execução completa do pipeline para entrega dos produtos finais.


## 📋 Regras de Negócio Aplicadas

    🔄 Dependências entre Campos
    As regras de tratamento consideram relações lógicas entre campos, respeitando critérios legais e atuariais:

    CO_TIPO_FUNDO: Classificação entre FUNPREV (1) e FUNFIN (2) com base nos Decretos Municipais nº 61.151/2022 e nº 64.144/2025:

    FUNFIN (2): DT_ING_ENTE ≤ 27/12/2018, DT_NASC_SERVIDOR > 28/02/1957, IN_PREV_COMP == "2"
    FUNPREV (1): Todos os demais casos, incluindo IN_PREV_COMP == "1"

    DT_ING_SERV_PUB, DT_ING_CARGO, DT_ING_CARREIRA dependem de DT_ING_ENTE
    DT_ING_ENTE depende de DT_NASC_SERVIDOR
    VL_CONTRIBUICAO depende de VL_BASE_CALCULO
    NU_TEMPO_RGPS limitado ao teto atuarial de 22.280 meses


⚠️ Gatilhos de Inconsistência

    Os ajustes são aplicados somente em registros que apresentarem inconsistência.
    As validações são realizadas por funções específicas que detectam anomalias antes de aplicar qualquer transformação.
    Registros válidos seguem diretamente para .gold; registros inconsistentes passam por .silver.


## 🧾 Regras Específicas por Tipo de Base

    🧑‍💼 Servidores Ativos

    CO_TIPO_FUNDO: Registros com código 9 são segregados como servidores cedidos.
    CO_TIPO_PODER: Corrigir para 1 (Administração Direta) quando CO_PODER ≠ 1.
    CO_CRITERIO_ELEGIBILIDADE: Preencher com 1 (Sem Critério Diferenciado) se estiver vazio.
    DT_ING_ENTE: Ajustar para garantir idade mínima de ingresso de 25 anos.
    DT_ING_SERV_PUB, DT_ING_CARREIRA, DT_ING_CARGO: Preencher em branco com base em DT_ING_ENTE.
    VL_BASE_CALCULO, VL_REMUNERACAO: Corrigir valores abaixo do salário mínimo ou acima do teto.
    VL_CONTRIBUICAO: Manter em branco para códigos funcionais 3 ou 11; recalcular como 14% de VL_BASE_CALCULO nos demais casos.
    NU_TEMPO_RGPS: Limitar ao teto de 22.280 meses.

    🧓 Servidores Aposentados (em fase de implementação)

    CO_TIPO_PODER: Corrigir para 1 (Administração Direta) quando CO_PODER ≠ 1.
    CO_TIPO_APOSENTADORIA: Atribuir 2 (Tempo de Contribuição) ou 4 (Invalidez) conforme tipo.
    DT_ING_ENTE e DT_ING_SERV_PUB: Ajustar para garantir idade mínima de ingresso.
    VL_APOSENTADORIA: Corrigir valores fora dos limites legais.
    VL_CONTRIBUICAO: Preencher com 14% da aposentadoria ou ajustar valores excedentes.

    👨‍👧 Pensionistas (em fase de implementação)

    CO_TIPO_RELACAO: Preencher em branco com código 6 (Outros).
    DT_INICIO_PENSAO: Ajustar se anterior à data de nascimento do pensionista.
    VL_BENEF_PENSAO: Corrigir valores fora dos limites e redistribuir proporcionalmente.
    VL_TOT_PENSAO: Somar valores individuais por instituidor, respeitando o teto.
    VL_PCT_QUOTA: Recalcular proporcionalmente ao valor individual.
    CO_CONDICAO: Atribuir 2 (Inválido) para filhos inválidos e 1 (Válido) para os demais.
    CO_DURACAO: Atribuir 2 (Temporário) para menores de 21 anos e 1 (Vitalício) para os demais.
    NU_TEMPO_DURACAO: Preencher com tempo restante até completar 21 anos para pensionistas temporários.

## 🚀 Instruções Básicas de Uso

### 📁 Preparação

Coloque os arquivos .xlsx originais na pasta .raw, seguindo o padrão de nome:
servidor_AAAA_MM.xlsx, aposentados_AAAA_MM.xlsx, pensionistas_AAAA_MM.xlsx.
Certifique-se de que o ambiente virtual está ativado e os pacotes do requirements.txt estão instalados.

Os dados ajustados serão salvos em:
    .silver (formato Parquet) para registros intermediários.
    .gold (formato Excel) para os resultados finais.

Os logs de execução são registrados em logs/integrador.log.

### 🚀 Execução
    Ative o ambiente virtual e execute os scripts:
    Shellvenv\Scripts\activate 
    .\scripts\integrador.py

### 📌 Observações

    Os arquivos devem seguir o padrão de nome: "servidor_AAAA_MM.xlsx", "aposentado_AAAA_MM.xlsx", etc.
    O pipeline pode ser expandido com novos stepXX.py conforme regras de negócio.
    Um script integrador (run_pipeline.py) será criado para executar todas as etapas em sequência.


## 📤 Autor
    Lucas Alves Gouveia
    Diretor Técnico de Divisão
    IPREM-SP

    