# ADR-011: Column mapping mode `name` na Bronze

- Status: Accepted
- Date: 2026-05-14

## Context

A tabela Bronze foi criada com `delta.columnMapping.mode = 'name'` nas TBLPROPERTIES. Esta é uma decisão de baixa visibilidade (uma linha na DDL) mas com implicações relevantes:

- Habilita operações que sem ela exigem rewrite completo da tabela: `RENAME COLUMN`, `DROP COLUMN`.
- Permite nomes de coluna com caracteres especiais (espaços, vírgulas, alguns símbolos) que sem column mapping são proibidos no Parquet.
- Eleva requisitos para `delta.minReaderVersion = '2'` e `delta.minWriterVersion = '5'`.

Existem três modes:

1. **`'none'`** (default): nome da coluna na tabela é exatamente o nome no Parquet físico.
2. **`'id'`**: cada coluna recebe ID interno; nome lógico e físico desacoplados via ID.
3. **`'name'`**: nome lógico e físico desacoplados via mapeamento por nome.

Pelo ADR-010, esperamos schema evolution recorrente. Pelo ADR-012, já dependemos de reader version 2+ por causa do Liquid Clustering — então o trade-off de versão já está pago.

## Decision

A bronze usa **`delta.columnMapping.mode = 'name'`** desde a criação da tabela.

**Por que essa escolha?**
Porque o custo é zero (uma propriedade no `CREATE TABLE`) e o benefício é assimétrico: sem column mapping, qualquer `RENAME COLUMN` ou `DROP COLUMN` futuro exige `OVERWRITE` da tabela inteira, o que em uma bronze com centenas de milhões de linhas é caro e arriscado. Com `name`, essas operações viram metadata-only. Como o ADR-012 já nos obriga a reader version 2+ via Liquid Clustering, não há trade-off adicional de compatibilidade — o "custo" da column mapping já está pago de outra forma. Não habilitar essa flag desde o início é abrir mão de flexibilidade futura sem ganhar nada.

## Consequences

### Positivas

- `ALTER TABLE ... RENAME COLUMN antigo TO novo` vira metadata-only — útil se descobrirmos que `_source_year` deveria ter sido `_partition_year`, ou se TLC renomear coluna entre publicações.
- `ALTER TABLE ... DROP COLUMN` também metadata-only — possibilita remover colunas obsoletas sem rewrite. Útil se uma coluna fictícia foi adicionada via permissive schema evolution (ADR-010) e precisamos limpar.
- Suporte a nomes não-padrão se algum dia for necessário (improvável neste case, mas barato).
- Time travel funciona consistentemente após rename/drop — Delta resolve via mapping, não por nome literal no histórico.

### Negativas (trade-offs)

- **Reader version 2 obrigatória**: leitores Delta antigos não conseguem ler. Já é requisito do Liquid Clustering (ADR-012), então custo marginal aqui é zero — mas vale registrar que essa dependência se intensifica.
- **Não é reversível trivialmente**: uma vez habilitado, não há comando "desligar". Voltar para `'none'` exige criar tabela nova e copiar dados.
- **Ferramentas externas que leem Parquet diretamente** (bypass do Delta log) podem ficar confusas — column mapping renomeia campos no nível físico. Mas o caso de uso "ler Parquet bypass" é antipattern em tabelas Delta de qualquer jeito.
- Pequena complexidade adicional em debug: `SHOW COLUMNS` mostra nome lógico, mas inspeção direta dos Parquets físicos mostra nomes mapeados (`col-<uuid>` ou similar). Raramente relevante, mas surpreende quem não conhece.

## Alternatives

### Rejeitada: `columnMapping.mode = 'none'` (default)

Não habilitar column mapping.

**Por que não a alternativa óbvia?**
"Default é seguro" é uma boa heurística, mas falha aqui pelos seguintes motivos:

1. **A dependência de reader version 2+ já existe** via Liquid Clustering — não estamos ganhando portabilidade ao manter `none`.
2. **Operações que poderiam ser metadata-only viram rewrite**. Em tabela bronze pequena (15M linhas) isso é tolerável; em bronze de 2B linhas é dor.
3. **Não habilitar agora é difícil de habilitar depois**: enable column mapping em tabela existente exige `ALTER TABLE ... SET TBLPROPERTIES` com cuidado e às vezes rewrite parcial. Mais simples ligar desde a criação.

### Rejeitada: `columnMapping.mode = 'id'`

Modo equivalente, mas mapeia colunas por ID interno em vez de nome.

**Por que não essa alternativa?** Diferença prática: `id` é mais robusto a renames frequentes mas requer reader version superior em algumas configurações. Para o caso atual (bronze que vai ter rename ocasional, não constante), `name` é suficiente e ligeiramente mais amigável em debug porque `DESCRIBE TABLE` mostra mapping legível. `id` faria mais sentido para tabelas com schema muito volátil ou para self-managed pipelines onde rename é frequente.

### Outras consideradas

- **Não declarar TBLPROPERTIES de versão**: deixar Delta inferir min reader/writer das features usadas. Funciona, mas torna a configuração implícita — preferimos declarar explicitamente para que `DESCRIBE EXTENDED` mostre o contrato sem ambiguidade.

## Validation

Critérios de validação contínua:

- `DESCRIBE EXTENDED <table>` deve mostrar `delta.columnMapping.mode = name` nas properties.
- Testar `ALTER TABLE ... RENAME COLUMN` em ambiente dev: deve completar em segundos (metadata-only), não em minutos (rewrite).
- Verificar que ferramentas downstream (Databricks SQL, notebooks, dbt) continuam lendo a tabela normalmente após rename/drop — column mapping é transparente para SQL.
- Time travel após rename deve continuar funcionando: `SELECT * FROM <table> VERSION AS OF X` antes e depois do rename deve ser consistente.

**Quando essa decisão deve ser revisitada?**

- Quando a tabela precisar ser exposta para ferramenta externa sem suporte a Delta reader 2+ (Athena/Trino versões antigas, conectores legados) — pode forçar revisitar a stack inteira.
- Quando rename/drop começar a ser frequente o suficiente para justificar `'id'` mode em vez de `'name'`.
- Quando uma future feature do Delta exigir column mapping mode específico — monitorar release notes.