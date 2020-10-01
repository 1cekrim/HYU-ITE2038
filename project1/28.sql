SELECT
    Trainer.name,
    AVG(CatchedPokemon.level) AS avgLevel
FROM
    Trainer,
    CatchedPokemon,
    Pokemon
WHERE
    CatchedPokemon.owner_id = Trainer.id
    AND CatchedPokemon.pid = Pokemon.id
    AND (
        Pokemon.type = 'Normal'
        OR Pokemon.type = 'Electric'
    )
GROUP BY
    Trainer.id
ORDER BY
    avgLevel;