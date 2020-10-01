SELECT
    City.name,
    cityAvgLevel.avgLevel
FROM
    City
    LEFT OUTER JOIN (
        SELECT
            City.name,
            AVG(CatchedPokemon.level) AS avgLevel
        FROM
            City,
            CatchedPokemon
        WHERE
            CatchedPokemon.owner_id IN (
                SELECT
                    Trainer.id
                FROM
                    Trainer
                WHERE
                    Trainer.hometown = City.name
            )
        GROUP BY
            City.name
    ) AS cityAvgLevel ON cityAvgLevel.name = City.name
ORDER BY
    avgLevel;