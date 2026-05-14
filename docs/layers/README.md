# Documentacao das Camadas Lakehouse

Este diretorio descreve o estado atual de implementacao das camadas do pipeline:

- [Landing](./landing.md)
- [Bronze](./bronze.md)
- [Silver](./silver.md)
- [Gold](./gold.md)

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

- `landing`: implementada, com modo explicito e discovery.
- `bronze`: implementada, com Auto Loader + Delta + checkpoint.
- `silver`: placeholder (nao implementada).
- `gold`: placeholder (nao implementada).

## Template usado nos documentos

Cada camada segue este formato:

1. Papel da camada no pipeline.
2. Implementacao atual (fatos observaveis no codigo).
3. Fluxo de execucao.
4. Entradas e saidas.
5. Riscos tecnicos.
6. Proximos passos sugeridos.
