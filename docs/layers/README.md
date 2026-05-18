# Documentacao das Camadas Lakehouse

Este diretorio descreve o estado atual de implementacao das camadas do pipeline:

- [Landing](./landing.md)
- [Bronze](./bronze.md)
- [Silver](./silver.md)
- [Gold](./gold.md)
- [Consumo](./consumo.md)

## Objetivo

Padronizar a documentacao tecnica para:

- onboarding de engenharia;
- revisao arquitetural;
- planejamento de evolucoes;
- rastreabilidade entre codigo, operacao e ADRs.

## Convencoes desta pasta

- O conteudo descreve primeiro o que ja esta implementado.
- Opiniao e proposta de melhoria ficam separadas em "Riscos" e "Proximos passos".
- Comandos de exemplo usam os entrypoints reais dos wheels (`ingest_*`).
- Referencias de codigo sempre apontam para `src/nyc_taxi/lakehouse/<camada>/main.py`.

## Estado atual (resumo)

- `landing`: implementada, multi-taxi (yellow + green), com modo explicito
  e discovery, isolamento por subpath e politica de falha parcial.
- `bronze`: implementada, multi-taxi (uma tabela Delta por taxi),
  Auto Loader + checkpoint independentes por taxi.
- `silver`: implementada, uma tabela Delta por taxi (`yellow_taxi_trips`,
  `green_taxi_trips`), MERGE idempotente com schema evolution, DQ inline
  ancorada nos data dictionaries do TLC e politica fail-fast. Modelagem
  detalhada no ADR-013.
- `gold`: implementada, uma unica view de consumo (`vw_taxi_trips`)
  que une yellow + green com alias `lpep_*` -> `tpep_*` e expoe as
  colunas obrigatorias do contrato de consumo. Sem fatos pre-agregados
  — as perguntas analiticas rodam como SQL ad-hoc contra a view.
  Modelagem detalhada no ADR-014.
- `consumo`: Databricks Genie Space configurado sobre `vw_taxi_trips`,
  permitindo responder as perguntas analiticas via linguagem natural
  sem dashboard ou infraestrutura adicional de BI.

## Template usado nos documentos

Cada camada segue este formato:

1. Papel da camada no pipeline.
2. Implementacao atual (fatos observaveis no codigo).
3. Fluxo de execucao.
4. Entradas e saidas.
5. Riscos tecnicos.
6. Proximos passos sugeridos.
