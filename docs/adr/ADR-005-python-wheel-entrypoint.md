# ADR-005: Executar ingestão via python_wheel entry point

- Status: Accepted
- Date: 2026-05-13

## Context

O processo de ingestão landing precisa ser executável de forma repetível em job, testável localmente e versionável como código Python. O projeto já está estruturado como pacote com `pyproject.toml` e scripts de build/deploy.

Precisamos de um mecanismo de execução que:

- evite dependência de estado de notebook;
- facilite testes unitários e automação em CI;
- mantenha parâmetros explícitos por linha de comando.

## Decision

A ingestão será executada por uma **python_wheel task** com entry point `main(argv)` e parsing por `argparse` (em vez de lógica acoplada a notebook/widgets).

**Por que essa escolha?**  
Porque entry point em wheel mantém o pipeline como aplicação Python versionada, melhora testabilidade e reduz variáveis implícitas de execução.

## Consequences

### Positivas

- `main()` pode ser chamado em testes sem precisar de notebook runtime.
- Parâmetros ficam explícitos (`--target-year`, `--target-months`, etc.).
- Deploy e rollback seguem artefato versionado (wheel), mais previsível em produção.

### Negativas (trade-offs)

- Exige disciplina de empacotamento e manutenção de entry points.
- Menos conveniente para exploração ad hoc do que notebooks interativos.
- Requer mocks de `spark/dbutils` em testes para cobrir caminhos de execução.

## Alternatives

### Rejeitada: manter notebook como unidade principal de execução

**Por que não a alternativa óbvia?**  
Notebook é ótimo para exploração, mas como unidade principal de produção aumenta acoplamento ao ambiente interativo, dificulta testes automatizados e favorece configuração implícita (widgets/estado de sessão).

### Outras consideradas

- Script solto sem empacotamento: simples no início, mas mais frágil para distribuição/versionamento.
- Workflows com tasks SQL apenas: não cobre bem a lógica imperativa de download e metadados.

## Validation

Critérios de validação contínua:

- Entry point roda com sucesso em task `python_wheel` com parâmetros declarados.
- Testes unitários cobrem fluxo de sucesso/falha sem depender de notebook.
- Mudanças de versão do pacote refletem corretamente no deploy do job.

**Quando essa decisão deve ser revisitada?**

- quando a complexidade de packaging superar o benefício de governança e teste;
- quando o time priorizar desenvolvimento predominantemente notebook-driven;
- quando houver plataforma padrão interna diferente para orquestração de jobs Python.
