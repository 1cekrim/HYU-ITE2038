SELECT
    Pokemon.name
FROM
    Pokemon
WHERE
    Pokemon.id NOT IN (
        SELECT
            CatchedPokemon.pid
        FROM
            CatchedPokemon,
            Trainer
        WHERE
            CatchedPokemon.owner_id = Trainer.id
    )
ORDER BY
    Pokemon.name;