# 📊 Pipeline_ajustes_atuarial

    Projeto de engenharia de dados voltado para o tratamento, ajuste e qualificação de bases atuariais do RPPS do Município de São Paulo, com foco em servidores ativos, aposentados e pensionistas. O pipeline realiza validações, correções e ajustes conforme regras de negócio e legislação vigente, visando entregar produtos finais consistentes e auditáveis.

---

## 🗂️ Estrutura de Diretórios

    pipeline_ajustes_atuarial/
    ├── .raw/               # Dados originais (.xlsx)
    ├── .silver/            # Dados intermediários tratados (formato Parquet)
    ├── gold/              # Dados finais ajustados (.xlsx)
    ├── iconfig/            # Arquivos de configuração (YAML/JSON)
    ├── logs/               # Logs de execução e auditoria
    ├── notebooks/          # Análises exploratórias e testes
    ├── scripts/            # Scripts de tratamento por tipo de base
    ├── venv/               # Ambiente virtual Python
    ├── README.md           # Documentação do projeto
    └── requirements.txt    # Dependências do projeto

---

## ⚙️ Tecnologias Utilizadas

    Python 3.10+
    Pandas e PyArrow (manipulação de dados e Parquet)
    OpenPyXL (leitura/escrita de arquivos Excel)
    Dateutil (manipulação de datas)
    VSCode (ambiente de desenvolvimento)
    PowerShell / WSL (execução em ambiente Windows)

---

## 📅 Plano de Desenvolvimento – 7 Fases

    ### ✅ Fase 1 – Estruturação do Ambiente (concluída)
    - Criação da estrutura de diretórios

    ### ✅ Fase 2 – Criação do Ambiente Virtual (concluída)
    - Criação do ambiente com `venv`
    - Instalação de pacotes essenciais

    ### 🔄 Fase 3 – Definição de Parâmetros e Regras
    - Criar `iconfig/parametros.yaml`
    - Documentar regras de negócio por tipo de base

    ### 🔄 Fase 4 – Scripts de Leitura e Validação Inicial
    - Leitura dos arquivos `.xlsx`
    - Validação de estrutura e campos obrigatórios
    - Geração de logs de inconsistência

    ### 🔄 Fase 5 – Tratamentos e Ajustes
    - Implementar regras de tratamento por base
    - Salvar resultados na camada `.silver`

    ### 🔄 Fase 6 – Geração da Camada Gold
    - Consolidar dados tratados
    - Aplicar filtros finais e exportar para consumo

    ### 🔄 Fase 7 – Documentação e Testes
    - Finalizar `README.md`
    - Criar notebooks de teste
    - Validar execução completa do pipeline

---

## 📋 Regras de Negócio Aplicadas (Definição Inicial)
    🔄 Dependência entre campos

    CO_TIPO_FUNDO depende de DT_NASC_SERVIDOR, DT_ING_ENTE, IN_PREV_COMP
    DT_ING_SERV_PUB e DT_ING_CARGO dependem de DT_ING_ENTE
    DT_ING_ENTE depende de DT_NASC_SERVIDOR
    VL_CONTRIBUICAO depende de VL_BASE_CALCULO

    ⚠️ Gatilhos de inconsistência

    Ajustes são aplicados somente em registros que apresentarem inconsistência.
    Validações são feitas por funções específicas que detectam anomalias antes de aplicar qualquer transformação.


    🧾 Regras Específicas por Tipo de Base
    Servidores Ativos

    CO_TIPO_FUNDO: Registros com código 9 serão segregados em aba específica (servidores cedidos).
    CO_TIPO_PODER: Corrigir para 1 – Administração Direta quando CO_PODER for diferente de 1.
    CO_CRITERIO_ELEGIBILIDADE: Preencher com 1 – Sem Critério Diferenciado para registros em branco.
    DT_ING_SERV_PUB / DT_ING_ENTE: Ajustar para garantir idade mínima de ingresso de 25 anos.
    DT_ING_CARREIRA / DT_ING_CARGO: Preencher registros em branco com a data ajustada de DT_ING_ENTE.
    VL_BASE_CALCULO / VL_REMUNERACAO: Corrigir valores abaixo do salário mínimo ou acima do teto.
    VL_CONTRIBUICAO: Manter em branco para servidores com códigos funcionais 3 ou 11.
    NU_TEMPO_RGPS: Limitar tempo de contribuição ao teto atuarial de 22.280 meses.

    Servidores Aposentados

    CO_TIPO_PODER: Corrigir registros com CO_PODER diferente de 1 para CO_TIPO_PODER = 1 – Administração Direta.
    CO_TIPO_APOSENTADORIA: Atribuir 2 – Tempo de Contribuição para aposentadorias válidas e 4 – Invalidez para inválidas.
    DT_ING_SERV_PUB / DT_ING_ENTE: Ajustar datas para garantir idade mínima de ingresso de 25 anos.
    VL_APOSENTADORIA: Corrigir valores abaixo do salário mínimo ou acima do teto específico.
    VL_CONTRIBUICAO: Preencher com 14% da aposentadoria ou ajustar valores excedentes.

    Pensionistas

    CO_TIPO_RELACAO: Preencher registros em branco com código 6 – Outros.
    DT_INICIO_PENSAO: Ajustar datas que sejam anteriores à data de nascimento do pensionista.
    VL_BENEF_PENSAO: Identificar valores abaixo do salário mínimo ou acima do teto; redistribuir proporcionalmente entre pensionistas do mesmo instituidor.
    VL_TOT_PENSAO: Calcular somando os valores individuais por instituidor, respeitando limites legais.
    VL_PCT_QUOTA: Recalcular proporcionalmente ao valor individual da pensão.
    CO_CONDICAO: Atribuir 2 – Inválido para filhos inválidos e 1 – Válido para os demais.
    CO_DURACAO: Atribuir 2 – Temporário para menores de 21 anos e 1 – Vitalício para os demais.
    NU_TEMPO_DURACAO: Preencher com o tempo restante (em anos) até completar 21 anos para pensionistas temporários.


## 🚀 Instruções Básicas de Uso

    Coloque os arquivos .xlsx originais na pasta .raw
    Execute os scripts em scripts/ conforme o tipo de base
    Os dados ajustados serão salvos em .silver (Parquet) e .gold (Excel)
    Verifique os logs em logs/ para rastreabilidade
    Consulte os parâmetros em iconfig/ para ajustes finos


## 📌 Recomendações Adicionais

    Utilize versionamento com Git para controle de alterações
    Documente cada regra aplicada nos scripts
    Gere relatórios de validação por CPF/matrícula
    Mantenha backups dos arquivos .raw e .gold
    Automatize a geração de relatórios finais para entrega
    Priorize modularidade e reutilização de funções