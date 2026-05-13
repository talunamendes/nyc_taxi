# ADR-001: Usar Unity Catalog Volume como Landing Zone

- Status: Accepted
- Date: 2026-05-13

## Context

O pipeline de ingestão de NYC Taxi precisa de um armazenamento bruto para arquivos Parquet antes das camadas Bronze/Silver/Gold. O ambiente alvo inclui Databricks com Unity Catalog e limitações típicas de setup local/free tier para integrações externas.

Precisamos de uma solução que:

- funcione com permissões e governança do Unity Catalog;
- seja simples de operar no contexto atual do projeto;
- permita organização por partições (`year=/month=`) para facilitar processamento downstream.

## Decision

A landing zone será implementada em **Unity Catalog Volume** (`/Volumes/<catalog>/<schema>/<volume>`), com partições Hive-style por ano e mês.

**Por que essa escolha?**  
Porque UC Volume entrega integração nativa com o ambiente Databricks do projeto, reduz atrito operacional inicial e mantém governança no mesmo plano de controle já usado pelo pipeline.

## Consequences

### Positivas

- Menor esforço de bootstrap para desenvolvimento e execução no Databricks.
- Governança e namespace consistentes com catálogo/esquemas já definidos.
- Estrutura de paths previsível para consumo pelas próximas camadas.

### Negativas (trade-offs)

- Maior acoplamento ao ecossistema Databricks/Unity Catalog.
- Menor portabilidade imediata para execução fora desse ambiente.
- Pode exigir revisão se requisitos de capacidade/rede evoluírem para padrões multi-cloud mais rígidos.

### Comparação explícita: UC Volume vs S3

- **Controle de acesso:** UC Volume é forte para governança Databricks-first (catálogo/esquema/volume e grants por principal), mas S3 oferece controles mais amplos para cenários multi-serviço e multi-conta (IAM, bucket policies, SCPs e controles de rede).
- **Ciclo de vida:** S3 possui lifecycle nativo maduro (tiering, expiração, retenção e object lock). UC Volume não entrega o mesmo conjunto de lifecycle como capacidade principal no plano de governança do projeto.
- **Versionamento:** S3 tem versionamento de objeto nativo. Em UC Volume, versionamento não é a primitiva principal exposta ao time de dados no mesmo nível operacional.
- **Criptografia:** ambos suportam criptografia em trânsito e em repouso; S3 tende a oferecer maior transparência e granularidade operacional para políticas de KMS e compliance.
- **Custo:** base de armazenamento tende a ser semelhante por usar object storage subjacente, mas o custo total depende de requests, transferência e compute. UC Volume não implica redução automática de custo.

## Alternatives

### Rejeitada: usar bucket S3 como landing principal

**Por que não a alternativa óbvia?**  
S3 é uma alternativa natural para data lake, mas foi rejeitada neste momento porque adiciona complexidade de credenciais, políticas e integração para o estágio atual do projeto. O ganho de padronização não compensa o custo operacional imediato no contexto atual.

Observação: esta rejeição é contextual ao estágio atual do projeto e não significa inferioridade técnica do S3 para todos os cenários.

### Outras consideradas

- DBFS tradicional: simples, mas menos alinhado à governança explícita por catálogo/esquema.
- Armazenamento local efêmero: inviável para execução recorrente e auditável.

## Validation

Critérios de validação contínua:

- Ingestão cria e escreve com sucesso em paths de volume esperados.
- Times consumidores conseguem ler dados por partições sem workarounds.
- Operação não depende de configurações manuais extras fora do bundle/pipeline.

**Quando essa decisão deve ser revisitada?**

- quando houver requisito explícito de portabilidade multi-cloud entre engines;
- quando custo/performance do volume se tornar gargalo recorrente;
- quando governança corporativa exigir landing em storage externo padronizado (ex.: S3/ADLS/GCS).
- quando houver exigência formal de lifecycle/versionamento no nível de objeto como requisito de auditoria ou retenção legal.
