# ADR-004: Usar Declarative Automation Bundles (DAB) para CI/CD

- Status: Accepted
- Date: 2026-05-13

## Context

O projeto já possui `databricks.yml` com definição de bundle, targets (`dev` e `prod`) e build de artefato wheel. Precisamos padronizar a estratégia de CI/CD para deploy e promoção entre ambientes.

Além disso, o workflow foi configurado para **Serverless Jobs**, o que exige declarar dependências Python no bloco `environments.spec.dependencies` (não em `tasks[].libraries`).

A solução deve:

- manter infra de jobs/pipelines versionada junto ao código;
- suportar múltiplos ambientes com diferenças controladas;
- reduzir deriva manual entre ambientes Databricks;
- permitir automação por pipeline externa quando necessário.

## Decision

Adotar **Declarative Automation Bundles (DAB)** como mecanismo principal de definição e deploy de recursos (Jobs/Pipelines/artefatos), usando `databricks bundle validate/deploy/run/destroy` como interface padrão.

Para jobs serverless, padronizar dependências da aplicação na `environment` do job e usar variável de bundle (`wheel_file`) para resolver dinamicamente o nome da wheel gerada em `dist/`.

**Por que essa escolha?**  
Porque DAB oferece modelo declarativo nativo da plataforma Databricks, com suporte explícito a targets e validação, reduzindo inconsistência de deploy e alinhando código + configuração no mesmo ciclo de versionamento. No contexto serverless, esse modelo também força a configuração correta de ambiente/dependências no próprio job.

Referência oficial: [Databricks Declarative Automation Bundles](https://docs.databricks.com/en/dev-tools/bundles/).

## Consequences

### Positivas

- Deploy reprodutível e declarativo por ambiente (`dev`, `prod`).
- Menor risco de configurações manuais divergentes no workspace.
- Melhor rastreabilidade de mudanças de infraestrutura de dados no Git.
- Integração natural com artefatos Python (wheel) e recursos Databricks.
- Fluxo operacional único para ciclo de vida do bundle (`validate`, `deploy`, `run`, `destroy`).

### Negativas (trade-offs)

- Acoplamento maior ao ecossistema Databricks e ao modelo de bundle.
- Curva de aprendizado para estrutura de bundle e convenções de targets.
- Cobertura focada em recursos Databricks; integrações cross-cloud amplas podem exigir ferramentas complementares.
- Em serverless, dependências exigem modelagem específica no `environment`; erros de path da wheel quebram execução.

## Alternatives

### Rejeitada: CI/CD baseado apenas em scripts customizados + Databricks CLI REST/Jobs API

**Por que não a alternativa óbvia?**  
Scripts customizados parecem simples no início, mas tendem a crescer com lógica ad hoc de promoção, validação e drift handling. Isso aumenta custo de manutenção e risco operacional quando comparado ao modelo declarativo padrão do DAB.

### Considerada: Terraform como ferramenta principal de deploy de jobs/pipelines

Terraform é forte para governança ampla de infraestrutura, mas para este projeto o foco imediato é entregar pipeline Databricks com menor atrito operacional. O DAB oferece melhor aderência ao fluxo de aplicação de dados (bundle + artefato + run) no estágio atual.

### Rejeitada no estado atual: tasks com `existing_cluster_id`

**Por que não a alternativa óbvia?**  
Executar tasks em cluster existente simplifica o entendimento inicial, mas adiciona dependência de `cluster_id` por usuário e reduz portabilidade para quem clona o projeto. O modelo serverless reduz essa fricção de bootstrap no setup atual.

### Considerada: GitHub Actions com DAB (modelo híbrido)

Sim, **é possível e recomendado** em muitos cenários.  
GitHub Actions pode ser o **orquestrador de CI/CD** (gatilhos, matriz, gates, approvals) enquanto o DAB permanece como **mecanismo de deploy**. Neste caso, não é uma substituição direta ao DAB, e sim uma composição:

- GitHub Actions: orquestra validação, testes, policy gates e promoção;
- DAB: aplica o estado desejado no Databricks (`bundle validate/deploy/run`).

## Validation

Critérios de validação contínua:

- Pipeline executa `databricks bundle validate` em pull requests sem erros.
- Deploy para `dev` e `prod` ocorre por target, sem ajustes manuais pós-deploy.
- Recursos implantados no workspace correspondem ao estado declarado no bundle.
- Rollback/redeploy é possível reaplicando versão anterior do repositório.
- Jobs serverless executam com sucesso usando dependências declaradas em `environments.spec.dependencies` e wheel referenciada por variável.

**Quando essa decisão deve ser revisitada?**

- quando o escopo de infraestrutura extrapolar recursos Databricks e exigir ferramenta unificada multi-plataforma como padrão corporativo;
- quando requisitos de compliance exigirem controles que o fluxo atual de bundle não cubra adequadamente;
- quando o custo de manutenção de bundle crescer além do benefício de padronização.
- quando a estratégia de compute mudar (ex.: retorno para clusters dedicados) exigindo revisão das decisões de ambiente/dependências serverless.

