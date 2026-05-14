# Índice de ADRs

Este diretório contém os Architectural Decision Records (ADRs) do projeto.

- [ADR-001: Usar Unity Catalog Volume como Landing Zone](./ADR-001-unity-catalog-volume-landing.md)
- [ADR-002: Executar ingestão via python_wheel entry point](./ADR-002-python-wheel-entrypoint.md)
- [ADR-003: Permitir falha parcial por mês na ingestão](./ADR-003-partial-failure-policy.md)
- [ADR-004: Usar DAB para CI/CD](./ADR-004-dab-para-cicd.md)
- [ADR-005: Usar Serverless Jobs como compute](./ADR-005-serverless-vs-classic-cluster.md)
- [ADR-006: Estratégia de ingestão HTTP para Landing Zone](./ADR-006-http-requests-landing.md)
- [ADR-007: Auto Loader como estratégia de ingestão Bronze](./ADR-007-bronze-estrategia-ingestao.md)
- [ADR-008: Permissive schema evolution na Bronze](./ADR-008-schema-evolution-bronze.md)
- [ADR-009: Column mapping mode `name` na Bronze](./ADR-009-column-mapping-mode.md)
- [ADR-010: Estratégia de organização física por camada (Liquid Clustering a partir da Silver)](./ADR-010-clustering-strategy-by-layer.md)
- [ADR-011: Time window strategy e parametrização da Landing](./ADR-011-landing-time-window-strategy.md)

## Como revisar um ADR

Cada ADR deve responder explicitamente:

1. Por que essa escolha?
2. Por que não a alternativa óbvia?
3. Quando essa decisão deve ser revisitada?

Se não responder os três pontos, o ADR está incompleto.
