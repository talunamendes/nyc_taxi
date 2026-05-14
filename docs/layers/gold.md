# Gold Layer

## Papel da camada

A camada `gold` deve publicar datasets de consumo (BI, analise ad hoc e metricas
de negocio) em formato estavel para usuarios finais.

Arquivo de referencia: `src/nyc_taxi/lakehouse/gold/main.py`.

## Implementacao atual

Estado atual: placeholder.

O modulo possui apenas:

- funcao `main()`;
- `print("Hello, Gold Layer!")`;
- sem leitura da silver;
- sem tabelas agregadas;
- sem definicao de metricas.

## Entradas e saidas

Atualmente nao ha contrato de entrada/saida de dados para esta camada.

## Riscos tecnicos atuais

- Nao existe camada de consumo pronta para BI.
- Metricas de negocio nao estao centralizadas nem versionadas.
- SLA de frescor e de consistencia ainda nao pode ser medido.

## Proximos passos sugeridos

- Definir primeiro conjunto de tabelas gold (fato + dimensoes essenciais).
- Formalizar metricas de negocio e suas regras de calculo.
- Implementar estrategia de refresh (batch ou incremental).
- Criar testes de regressao para metricas criticas.
