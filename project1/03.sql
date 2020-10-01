--3
SELECT
    AVG(CatchedPokemon.level)
FROM
    CatchedPokemon
WHERE
    CatchedPokemon.owner_id IN (
        SELECT
            Trainer.id
        FROM
            Trainer
        WHERE
            Trainer.hometown = 'Sangnok City'
    )
    AND (
        SELECT
            Pokemon.type
        FROM
            Pokemon
        WHERE
            CatchedPokemon.pid = Pokemon.id
    ) = 'Electric';