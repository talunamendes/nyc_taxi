# ADR-003: Permitir falha parcial por mês na ingestão

- Status: Accepted
- Date: 2026-05-13

## Context

A ingestão é processada por mês e depende de fonte externa (NYC TLC CDN), sujeita a indisponibilidade temporária ou atraso de publicação. Uma falha em um mês não implica necessariamente falha em todos os demais.

Precisamos definir política de erro para o job:

- abortar imediatamente na primeira falha; ou
- continuar processando meses restantes e consolidar resultado ao final.

## Decision

A task continuará o processamento mesmo que um mês falhe; o job só retorna erro (`exit code 1`) quando **todos** os meses falham. Se ao menos um mês for `ingested` ou `skipped`, retorna sucesso (`exit code 0`) com sumário explícito.

**Por que essa escolha?**  
Porque maximiza disponibilidade de dados parciais úteis e reduz reprocessamento desnecessário em cenários de falha intermitente por partition.

## Consequences

### Positivas

- Mais resiliência operacional diante de falhas externas pontuais.
- Menor desperdício de tempo/custo: meses saudáveis continuam sendo entregues.
- Sumário final permite observabilidade de quantos meses falharam/sucesso.

### Negativas (trade-offs)

- Pode mascarar percepção de “job verde” com perdas parciais se monitoramento for fraco.
- Exige alertas e acompanhamento por contagem de falhas, não só status binário do job.
- Consumidores precisam lidar com completude parcial dos dados.

## Alternatives

### Rejeitada: fail-fast na primeira exceção

**Por que não a alternativa óbvia?**  
Fail-fast simplifica semântica de erro, mas interrompe processamento de meses potencialmente saudáveis, aumentando backlog e reexecuções. Para ingestão particionada, essa penalidade foi considerada maior que o benefício de simplicidade.

### Outras consideradas

- Retentativas globais da task inteira: pode repetir trabalho já concluído sem necessidade.
- Reprocessamento manual exclusivo: aumenta esforço operacional contínuo.

## Validation

Critérios de validação contínua:

- Logs e saída JSON sempre informam `ingested`, `skipped` e `failed`.
- Monitoramento alerta quando `failed > 0`, mesmo com `exit code 0`.
- Rotina de reprocessamento posterior consegue cobrir partições falhadas.

**Quando essa decisão deve ser revisitada?**

- quando contratos downstream exigirem completude total por janela de execução;
- quando SLAs de qualidade não aceitarem entrega parcial;
- quando houver mecanismo robusto de retries por partition que torne fail-fast mais seguro.
