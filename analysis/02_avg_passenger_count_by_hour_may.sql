-- Pergunta 2:
-- Qual a média de passageiros (passenger_count) por cada hora do dia
-- que pegaram táxi no mês de maio, considerando todos os táxis da frota?

SELECT
    pickup_hour,
    avg_passenger_count
FROM gold.avg_passenger_count_by_hour_may
ORDER BY pickup_hour;

-- Alternativa: calcular direto da Silver:
--
-- SELECT
--     HOUR(tpep_pickup_datetime) AS pickup_hour,
--     ROUND(AVG(passenger_count), 2) AS avg_passenger_count
-- FROM silver.yellow_taxi_trips
-- WHERE trip_year = 2023 AND trip_month = 5
-- GROUP BY HOUR(tpep_pickup_datetime)
-- ORDER BY pickup_hour;
