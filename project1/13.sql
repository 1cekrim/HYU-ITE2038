-- 13
SELECT
    Pokemon.name,
    Pokemon.id
FROM
    Pokemon
WHERE
    Pokemon.id in (
        SELECT
            CatchedPokemon.pid
        FROM
            CatchedPokemon
        WHERE
            (
                SELECT
                    Trainer.hometown
                FROM
                    Trainer
                WHERE
                    CatchedPokemon.owner_id = Trainer.id
            ) = 'Sangnok City'
    )
ORDER BY
    Pokemon.id;