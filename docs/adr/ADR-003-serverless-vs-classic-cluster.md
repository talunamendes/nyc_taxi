# ADR-003: Usar Serverless Jobs como compute

- Status: Accepted
- Date: 2026-05-13

## Context

O pipeline precisa ser simples de clonar/executar por diferentes pessoas, com baixo esforço de bootstrap em Databricks Free Edition e sem exigir configuração manual de cluster por usuário.

A decisão de compute impacta diretamente:

- tempo de setup inicial;
- portabilidade da execução entre usuários;
- custo operacional de manutenção do ambiente.

## Decision

Adotar **Serverless Jobs** como compute padrão das tasks do workflow, com `environment_key` e dependências definidas em `environments.spec.dependencies`.

**Por que essa escolha?**  
Porque reduz fricção de execução para quem clona o projeto (sem `cluster_id` por usuário), simplifica operação do pipeline e mantém foco na lógica de dados em vez de administração de cluster.

Referências oficiais:

- [Serverless compute for workflows](https://docs.databricks.com/en/compute/serverless/)
- [Configure the serverless environment](https://docs.databricks.com/en/compute/serverless/dependencies)

## Consequences

### Positivas

- Menor esforço de onboarding para novos usuários no workspace.
- Menor dependência de configuração manual de cluster existente.
- Menor risco de erro por `cluster_id` inválido/permissão de cluster.
- Execução mais consistente para o objetivo do projeto.

### Negativas (trade-offs)

- Dependências Python precisam ser modeladas no `environment` do job (não em `tasks[].libraries`).
- Erros de path/arquivo da wheel afetam instalação da lib no runtime.
- Menor controle fino de configuração de compute comparado a cluster clássico dedicado.

## Alternatives

### Rejeitada: usar Classic Cluster (`existing_cluster_id`) como padrão

**Por que não a alternativa óbvia?**  
Classic Cluster é uma opção comum e flexível, mas neste caso aumenta a fricção para quem clona o repositório: cada usuário precisa cluster próprio, permissões e `cluster_id` válido. Isso piora a portabilidade e adiciona erro operacional em um cenário onde se prioriza reprodutibilidade.

### Outras consideradas

- Job cluster dedicado por workflow: bom isolamento, mas maior esforço de parametrização/custo operacional para o objetivo atual.
- Estratégia híbrida (serverless em dev, classic em prod): possível, mas adiciona complexidade prematura para o escopo atual.

## Validation

Critérios de validação contínua:

- `make dab-deploy` e `make dab-run` funcionam sem informar `cluster_id`.
- Tasks executam em serverless com dependências instaladas via `environments.spec.dependencies`.
- Onboarding de novo usuário acontece com quickstart sem etapa manual de cluster.

**Quando essa decisão deve ser revisitada?**

- quando houver requisito de configuração de compute não atendido por serverless;
- quando políticas de compliance/rede exigirem isolamento específico de cluster clássico;
- quando análise de custo/performance mostrar vantagem clara e sustentada de classic cluster para o workload.
