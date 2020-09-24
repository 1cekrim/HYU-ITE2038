SELECT
    Trainer.name
FROM
    Trainer
WHERE
    Trainer.id IN (
        SELECT
            CatchedPokemon.owner_id
        FROM
            CatchedPokemon
        WHERE
            CatchedPokemon.level <= 10
    )
GROUP BY
    Trainer.name
ORDER BY
    Trainer.name;