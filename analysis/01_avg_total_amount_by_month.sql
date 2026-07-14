-- Pergunta 1:
-- Qual a média de valor total (total_amount) recebido em um mês
-- considerando todos os yellow táxis da frota?
--
-- Consome diretamente a tabela Gold pré-agregada (sem necessidade de
-- reprocessar a Silver). Usuários finais só precisam saber ler SQL.

SELECT
    trip_year,
    trip_month,
    avg_total_amount
FROM gold.avg_total_amount_by_month
ORDER BY trip_year, trip_month;

-- Alternativa: calcular direto da Silver (caso a Gold ainda não tenha
-- sido materializada), útil para validação ad-hoc:
--
-- SELECT
--     trip_year,
--     trip_month,
--     ROUND(AVG(total_amount), 2) AS avg_total_amount
-- FROM silver.yellow_taxi_trips
-- GROUP BY trip_year, trip_month
-- ORDER BY trip_year, trip_month;
