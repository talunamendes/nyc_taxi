# Índice de ADRs

Este diretório contém os Architectural Decision Records (ADRs) do projeto.

Ordem sugerida de leitura por **fase natural de decisão/implementação** (não por numeração):

## Fase 0 — Arquitetura e fundamentos

- [ADR-001: Escolha da arquitetura Lakehouse](./ADR-001-escolha-arquitetura-lakehouse.md)
- [ADR-002: Usar Unity Catalog Volume como Landing Zone](./ADR-002-unity-catalog-volume-landing.md)
- [ADR-003: Usar Serverless Jobs como compute](./ADR-003-serverless-vs-classic-cluster.md)
- [ADR-004: Usar DAB para CI/CD](./ADR-004-dab-para-cicd.md)
- [ADR-005: Executar ingestão via python_wheel entry point](./ADR-005-python-wheel-entrypoint.md)

## Fase 1 — Landing (ingestão da fonte)

- [ADR-006: Estratégia de ingestão HTTP para Landing Zone](./ADR-006-http-requests-landing.md)
- [ADR-007: Time window strategy e parametrização da Landing](./ADR-007-landing-time-window-strategy.md)
- [ADR-008: Permitir falha parcial por mês na ingestão](./ADR-008-partial-failure-policy.md)

## Fase 2 — Bronze (consolidação inicial no Lakehouse)

- [ADR-009: Auto Loader como estratégia de ingestão Bronze](./ADR-009-bronze-estrategia-ingestao.md)
- [ADR-010: Permissive schema evolution na Bronze](./ADR-010-schema-evolution-bronze.md)
- [ADR-011: Column mapping mode `name` na Bronze](./ADR-011-column-mapping-mode.md)

## Fase 3 — Silver/Gold (otimização de consumo)

- [ADR-012: Estratégia de organização física por camada (Liquid Clustering a partir da Silver)](./ADR-012-clustering-strategy-by-layer.md)
- [ADR-013: Data model da Silver — uma tabela por tipo de taxi](./ADR-013-silver-data-model-per-taxi.md)
- [ADR-014: Gold Data Model — uma view única de consumo (sem fatos pré-agregados)](./ADR-014-gold-data-model-per-question.md)

## Fase 4 — Governança e Qualidade

- [ADR-015: Governança e Qualidade de Dados — Validação Hardcoded como MVP](./ADR-015-governanca-qualidade-dados.md)

## Como revisar um ADR

Cada ADR deve responder explicitamente:

1. Por que essa escolha?
2. Por que não a alternativa óbvia?
3. Quando essa decisão deve ser revisitada?

Se não responder os três pontos, o ADR está incompleto.
