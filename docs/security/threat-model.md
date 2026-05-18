# Threat Model (Resumo)

## Escopo

- Pipeline de ingestão e transformação de dados NYC Taxi em Databricks.
- Execução por job serverless com artefato wheel.
- Armazenamento em Unity Catalog (schemas/volumes/tabelas).

## Ativos Críticos

- Dados brutos e transformados (landing/bronze/silver/gold).
- Credenciais de acesso Databricks/CLI.
- Configuração de deploy (bundle) e artefatos wheel.

## Principais Ameaças

- **Acesso não autorizado a dados** por permissões excessivas em catálogo/schema/volume.
- **Supply chain de dependências** via wheel/dependências Python.
- **Exfiltração de dados** por paths/configuração incorreta de permissões.
- **Manipulação de execução** por alteração indevida em job/bundle.
- **Perda de disponibilidade** por falha na fonte externa (NYC TLC) ou erro de deploy.

## Mitigações Adotadas

- Governança de dados via Unity Catalog.
- Deploy declarativo com Declarative Automation Bundles (DAB) (rastreável em Git).
- Execução com separação por ambiente (`dev`/`prod`).
- Idempotência e tratamento de falhas por partição de mês.
- Testes unitários para fluxo principal do entry point.

## Riscos Residuais

- Dependência da disponibilidade do endpoint externo de dados.
- Risco operacional em configuração manual de permissões no workspace.
- Necessidade de monitoramento ativo para falhas parciais.
