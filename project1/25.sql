SELECT
    Pokemon.name
FROM
    Pokemon,
    (
        SELECT
            sang.pid
        FROM
            (
                SELECT
                    CatchedPokemon.pid
                FROM
                    Trainer,
                    CatchedPokemon
                WHERE
                    Trainer.hometown = 'Sangnok City'
                    AND CatchedPokemon.owner_id = Trainer.id
            ) AS sang,
            (
                SELECT
                    CatchedPokemon.pid
                FROM
                    Trainer,
                    CatchedPokemon
                WHERE
                    Trainer.hometown = 'Brown City'
                    AND CatchedPokemon.owner_id = Trainer.id
            ) AS brown
        WHERE
            sang.pid = brown.pid
        GROUP BY
            sang.pid
    ) AS popo
WHERE
    popo.pid = Pokemon.id
ORDER BY
    Pokemon.name;