# ADR-001: Escolha da arquitetura Lakehouse

- Status: Accepted
- Date: 2026-05-17

## Context

O pipeline NYC Taxi precisa cobrir um fluxo completo de dados: ingestao de arquivos Parquet externos, persistencia de dado bruto, curadoria progressiva e publicacao para consumo analitico. Ao mesmo tempo, o projeto precisa ser:

- simples de executar por quem clona o repositorio;
- auditavel e reprocessavel quando houver erro de dados;
- evolutivo para novos meses, novos datasets e novos consumidores.

As decisoes anteriores ja consolidaram componentes do ecossistema Databricks/Delta (UC Volume na landing, Auto Loader na bronze, e estrategia por camadas bronze/silver/gold). Faltava explicitar a decisao arquitetural de alto nivel que conecta essas escolhas: **qual modelo de arquitetura de dados o projeto adota como padrao**.

## Decision

O projeto adota **arquitetura Lakehouse em camadas (Landing -> Bronze -> Silver -> Gold)** como base do pipeline.

**Por que essa escolha?**
Porque o Lakehouse combina vantagens necessarias para este contexto:

1. **Confiabilidade operacional de Data Lake**: armazenamento de baixo custo e append-friendly para dado bruto e historico.
2. **Confiabilidade semantica de Warehouse**: tabelas Delta com schema, transacoes ACID, versionamento e leitura SQL para consumo analitico.
3. **Evolucao incremental por camada**: ingestao, qualidade e modelagem ficam desacopladas, permitindo entregar valor cedo (bronze funcionando) sem bloquear evolucao de silver/gold.

Em termos praticos, a camada:

- **Landing** recebe arquivos como zona de aterrissagem;
- **Bronze** preserva fidelidade da fonte com rastreabilidade;
- **Silver** aplica curadoria, normalizacao e contratos semanticos;
- **Gold** publica dados prontos para BI e consumo de negocio.

## Consequences

### Positivas

- **Separacao clara de responsabilidades**: cada camada tem objetivo tecnico definido, reduzindo acoplamento entre ingestao e consumo analitico.
- **Reprocessamento mais seguro**: erro em regra de negocio na silver/gold nao exige baixar arquivos novamente; bronze preserva historico bruto.
- **Melhor governanca e auditabilidade**: Delta + Unity Catalog oferecem trilha de schema, metadata e controle de acesso no mesmo plano operacional.
- **Escalabilidade evolutiva**: novas regras de qualidade, agregacoes e datasets entram por extensao de camadas, sem redesenhar todo o pipeline.
- **Alinhamento com o ecossistema escolhido**: as ADRs anteriores ja pagaram o custo de adotar Databricks/Delta; Lakehouse maximiza o retorno dessa stack.

### Negativas (trade-offs)

- **Mais moving parts** que um pipeline de tabela unica: mais artefatos, mais contratos e mais pontos de observabilidade.
- **Custo de disciplina arquitetural**: sem ownership claro por camada, o projeto pode degradar para "bronze inchada" ou "silver sem contrato".
- **Maior necessidade de testes por fronteira**: qualidade passa a depender tambem de testes de transicao entre camadas, nao so de funcoes isoladas.
- **Dependencia de plataforma Delta-first**: portabilidade para engines sem suporte completo a Delta pode exigir adaptacoes.

## Alternatives

### Rejeitada: Arquitetura simplificada de camada unica

Ingerir e transformar diretamente em uma tabela final, sem separacao bronze/silver/gold.

**Por que nao a alternativa obvia?**
Para um projeto pequeno, camada unica parece mais rapida no dia 1. O problema e o dia 30: erros de transformacao viram retrabalho caro, investigacao de qualidade fica opaca e o pipeline perde capacidade de evoluir com seguranca. A economia inicial vira debito tecnico cedo demais.

### Rejeitada: Data Warehouse-first sem zona bruta historica

Modelagem analitica direta em tabelas curadas, com descarte rapido do dado bruto.

**Por que nao essa alternativa?**
Ela reduz capacidade de auditoria e de reproducao de processamento. Quando o produtor muda schema ou publica correcao retroativa, faltam insumos para replay confiavel. Para fonte externa sem contrato forte (como TLC), manter historico bruto faz parte da mitigacao de risco.

### Rejeitada: Data Lake "raw files only" sem Delta tables

Persistir somente arquivos em storage e fazer transformacoes ad hoc.

**Por que nao essa alternativa?**
Sem transacao, sem versionamento tabular e sem contrato de schema no ponto de consumo, a operacao fica mais fragil e mais dificil de governar. O ganho de simplicidade e ilusorio: a complexidade reaparece em codigo manual de controle.

### Outras consideradas

- **Data mesh completo desde o inicio**: arquitetura valida em organizacoes grandes, mas prematura para um pipeline unico e um dominio unico.
- **Lambda/Kappa architecture**: foco em streaming de baixa latencia, sem justificativa para a cadencia mensal da fonte atual.

## Validation

Criterios de validacao continua:

- Fluxo Landing -> Bronze deve ser idempotente e reexecutavel sem duplicacao logica.
- Alteracoes em regras da Silver nao devem exigir reingestao da Landing.
- Publicacoes Gold devem consumir somente dados curados (sem bypass da Bronze).
- Incidentes de qualidade devem ser investigaveis com rastreabilidade da origem (`_source_file`, `_ingestion_ts` ou equivalente).
- Novos datasets (ex: outras familias TLC) devem reutilizar o mesmo padrao de camadas sem redesign estrutural.

**Quando essa decisao deve ser revisitada?**

- Quando o volume e frequencia mudarem drasticamente (ex: ingestao sub-horaria com requisitos near-real-time).
- Quando houver exigencia forte de portabilidade para engine sem suporte Delta/UC.
- Quando o pipeline deixar de ser single-domain e surgir necessidade real de ownership federado (gatilho para data mesh).
- Quando o custo operacional das multiplas camadas superar o beneficio de auditabilidade e evolucao (indicador de sobreengenharia).
