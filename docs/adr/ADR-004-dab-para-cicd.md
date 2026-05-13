# ADR-004: Usar Databricks Asset Bundles (DAB) para CI/CD

- Status: Accepted
- Date: 2026-05-13

## Context

O projeto já possui `databricks.yml` com definição de bundle, targets (`dev` e `prod`) e build de artefato wheel. Precisamos padronizar a estratégia de CI/CD para deploy e promoção entre ambientes.

A solução deve:

- manter infra de jobs/pipelines versionada junto ao código;
- suportar múltiplos ambientes com diferenças controladas;
- reduzir deriva manual entre ambientes Databricks;
- permitir automação por pipeline externa quando necessário.

## Decision

Adotar **Databricks Asset Bundles (DAB)** como mecanismo principal de definição e deploy de recursos (Jobs/Pipelines/artefatos), usando `databricks bundle validate/deploy/run` como interface padrão.

**Por que essa escolha?**  
Porque DAB oferece modelo declarativo nativo da plataforma Databricks, com suporte explícito a targets e validação, reduzindo inconsistência de deploy e alinhando código + configuração no mesmo ciclo de versionamento.

## Consequences

### Positivas

- Deploy reprodutível e declarativo por ambiente (`dev`, `prod`).
- Menor risco de configurações manuais divergentes no workspace.
- Melhor rastreabilidade de mudanças de infraestrutura de dados no Git.
- Integração natural com artefatos Python (wheel) e recursos Databricks.

### Negativas (trade-offs)

- Acoplamento maior ao ecossistema Databricks e ao modelo de bundle.
- Curva de aprendizado para estrutura de bundle e convenções de targets.
- Cobertura focada em recursos Databricks; integrações cross-cloud amplas podem exigir ferramentas complementares.

## Alternatives

### Rejeitada: CI/CD baseado apenas em scripts customizados + Databricks CLI REST/Jobs API

**Por que não a alternativa óbvia?**  
Scripts customizados parecem simples no início, mas tendem a crescer com lógica ad hoc de promoção, validação e drift handling. Isso aumenta custo de manutenção e risco operacional quando comparado ao modelo declarativo padrão do DAB.

### Considerada: Terraform como ferramenta principal de deploy de jobs/pipelines

Terraform é forte para governança ampla de infraestrutura, mas para este projeto o foco imediato é entregar pipeline Databricks com menor atrito operacional. O DAB oferece melhor aderência ao fluxo de aplicação de dados (bundle + artefato + run) no estágio atual.

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

**Quando essa decisão deve ser revisitada?**

- quando o escopo de infraestrutura extrapolar recursos Databricks e exigir ferramenta unificada multi-plataforma como padrão corporativo;
- quando requisitos de compliance exigirem controles que o fluxo atual de bundle não cubra adequadamente;
- quando o custo de manutenção de bundle crescer além do benefício de padronização.

