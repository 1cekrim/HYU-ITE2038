-- 12
SELECT
    Pokemon.name,
    Pokemon.type
FROM
    Pokemon
WHERE
    Pokemon.id IN (
        SELECT
            CatchedPokemon.pid
        FROM
            CatchedPokemon
        WHERE
            CatchedPokemon.level >= 30
    )
GROUP BY
    Pokemon.name,
    Pokemon.type
ORDER BY
    Pokemon.name;