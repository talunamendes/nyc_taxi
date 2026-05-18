# Camada de Consumo

## Papel da camada

A camada de consumo expõe `nyc_taxi_<env>.gold.vw_taxi_trips` para
usuários de negócio via **Databricks Genie Space** — um agente de
linguagem natural que traduz perguntas em SQL e executa contra a view
sem que o usuário precise conhecer a estrutura da tabela ou escrever
código.

A adoção de Genie substitui a necessidade de dashboards para o escopo
atual de demonstração: as duas perguntas analíticas do projeto são
respondidas interativamente, sem infraestrutura adicional de BI.

## Implementação atual

### Objeto exposto

```
nyc_taxi_dev.gold.vw_taxi_trips
```

A view é o único objeto registrado no Genie Space. O schema exposto é:

| Coluna                  | Descrição                                         |
| ----------------------- | ------------------------------------------------- |
| `VendorID`              | Identificador do fornecedor da corrida            |
| `passenger_count`       | Número de passageiros                             |
| `total_amount`          | Valor total cobrado pela corrida (USD)            |
| `tpep_pickup_datetime`  | Timestamp de início da corrida                   |
| `tpep_dropoff_datetime` | Timestamp de término da corrida                  |
| `taxi_type`             | Tipo do táxi (`yellow` ou `green`)               |

### Configuração do Genie Space

O Genie Space foi criado manualmente na Databricks UI:

1. Acessar **Databricks Workspace → Genie** (menu lateral).
2. Criar novo Space e adicionar `nyc_taxi_dev.gold.vw_taxi_trips` como
   fonte de dados.
3. Opcionalmente, adicionar instruções contextuais (ex.: unidades das
   colunas, filtros padrão de data) para melhorar a qualidade das
   respostas geradas.

Não há arquivo de configuração versionado: o Genie Space é um recurso
gerenciado pela plataforma Databricks, provisionado via UI.

## Perguntas analíticas respondidas

As duas perguntas analíticas do projeto são respondidas diretamente
no Genie por linguagem natural. Exemplos de prompts e o SQL
equivalente gerado:

### Pergunta A — média de `total_amount` por mês (yellow)

**Prompt de exemplo:**
> "Qual a média do valor total das corridas de táxi amarelo por mês?"

**SQL equivalente:**

```sql
SELECT
    date_trunc('month', tpep_pickup_datetime) AS month_ref,
    AVG(total_amount)                          AS avg_total_amount,
    COUNT(*)                                   AS trips_count
FROM nyc_taxi_dev.gold.vw_taxi_trips
WHERE taxi_type = 'yellow'
GROUP BY 1
ORDER BY 1;
```

### Pergunta B — média de `passenger_count` por hora em mai/2023 (todos os táxis)

**Prompt de exemplo:**
> "Em maio de 2023, qual a média de passageiros por hora do dia, considerando todos os tipos de táxi?"

**SQL equivalente:**

```sql
SELECT
    hour(tpep_pickup_datetime) AS hour_of_day,
    AVG(passenger_count)       AS avg_passenger_count,
    COUNT(*)                   AS trips_count
FROM nyc_taxi_dev.gold.vw_taxi_trips
WHERE tpep_pickup_datetime >= TIMESTAMP('2023-05-01')
  AND tpep_pickup_datetime <  TIMESTAMP('2023-06-01')
  AND passenger_count IS NOT NULL
GROUP BY 1
ORDER BY 1;
```

O SQL de referência completo está em `analysis/perguntas_analiticas.sql`.

## Justificativa da abordagem

Genie foi adotado por simplicidade para demonstração:

- **Sem infraestrutura adicional**: nenhum dashboard, tabela de
  agregação prévia ou pipeline extra são necessários — o Genie consulta
  a view diretamente.
- **Acesso ad-hoc**: qualquer pergunta analítica sobre os dados pode
  ser respondida sem envolver a equipe de engenharia.
- **Custo zero de manutenção**: a camada gold já é o contrato de
  consumo; o Genie é apenas uma interface sobre ela.
- **Alinhamento com a diretriz anti-overengineering do projeto**:
  adicionar um dashboard ou camada de BI seria prematuro dado o escopo
  atual (demonstração, leitor único, ~18M linhas).

## Riscos técnicos atuais

- **Genie Space não é versionado**: a configuração vive apenas na
  Databricks UI. Mudanças no Space (instruções contextuais, fontes de
  dados) não são rastreáveis por git.
- **Qualidade das respostas depende do contexto configurado**: sem
  instruções adicionais no Space, o Genie pode gerar SQL que ignora
  filtros implícitos (ex.: `passenger_count IS NOT NULL`).
- **Permissão de acesso gerenciada separadamente**: o controle de
  quem pode acessar o Genie Space é feito na UI da Databricks, fora
  do Unity Catalog.

## Próximos passos sugeridos

- **Adicionar instruções contextuais ao Genie Space** descrevendo
  unidades, filtros padrão e semântica das colunas — melhora a
  precisão das respostas geradas.
- **Avaliar dashboard** (ex.: Databricks Lakeview) se o número de
  consumidores ou a frequência de acesso crescer — o SQL de referência
  em `analysis/` já está pronto para servir de base.
- **Versionar a configuração do Genie** via Databricks Asset Bundles
  quando o recurso estiver disponível em DAB, garantindo rastreabilidade.
