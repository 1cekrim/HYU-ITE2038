-- 17
SELECT
    COUNT(*)
FROM
    (
        SELECT
            Pokemon.name
        FROM
            Pokemon
        WHERE
            Pokemon.id IN (
                SELECT
                    CatchedPokemon.pid
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
            )
        GROUP BY
            Pokemon.name
    ) AS TMP;