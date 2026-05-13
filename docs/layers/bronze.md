# Bronze Layer - Analise de Implementacao

## Status atual

Camada ainda em estado inicial (placeholder). O entrypoint atual (`src/nyc_taxi/lakehouse/bronze/main.py`) apenas imprime uma mensagem e nao implementa transformacoes de dados.

## Escopo previsto para a analise desta camada

Quando implementada, esta analise deve cobrir:

1. Leitura da `landing` (arquivos particionados no volume).
2. Estrategia de inferencia/contrato de schema.
3. Escrita em tabela Delta Bronze.
4. Idempotencia e politica de reprocessamento.
5. Controles de qualidade de dados de entrada.
6. Observabilidade e metricas operacionais.

## Refactorings alvo (apos implementacao inicial)

- Isolar regras de parsing/normalizacao em modulos proprios.
- Introduzir testes de contrato (schema e colunas obrigatorias).
- Implementar checkpointing e watermark para ingestao incremental.
- Padronizar erros recuperaveis vs. erros fatais.
