# Silver Layer

## Papel da camada

A camada `silver` deve concentrar curadoria e qualidade dos dados antes da
modelagem analitica na gold.

Arquivo de referencia: `src/nyc_taxi/lakehouse/silver/main.py`.

## Implementacao atual

Estado atual: placeholder.

O modulo possui apenas:

- funcao `main()`;
- `print("Hello, Silver Layer!")`;
- sem leitura de bronze;
- sem escrita em tabela silver;
- sem regras de qualidade, dedupe ou normalizacao.

## Entradas e saidas

Atualmente nao ha contrato de entrada/saida de dados para esta camada.

## Riscos tecnicos atuais

- Pipeline para na bronze para qualquer caso de uso analitico.
- Regras de qualidade ficam indefinidas ou distribuidas em camadas erradas.
- Atrasa definicao de contratos semanticos e de monitoracao.

## Proximos passos sugeridos

- Definir tabela(s) silver e regras minimas de qualidade.
- Implementar leitura incremental da bronze (ex.: CDF ou watermark por data).
- Introduzir tratamento de registros invalidos (quarantine).
- Cobrir regras com testes automatizados.
