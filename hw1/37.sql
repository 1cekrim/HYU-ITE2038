SELECT
    SUBQ.name,
    SUBQ.levelSum
FROM
    (
        SELECT
            Trainer.id,
            Trainer.name,
            SUM(CatchedPokemon.level) AS levelSum
        FROM
            Trainer,
            CatchedPokemon
        WHERE
            Trainer.id = CatchedPokemon.owner_id
        GROUP BY
            Trainer.id
    ) AS SUBQ,
    (
        SELECT
            Trainer.id,
            Trainer.name,
            SUM(CatchedPokemon.level) AS levelSum
        FROM
            Trainer,
            CatchedPokemon
        WHERE
            Trainer.id = CatchedPokemon.owner_id
        GROUP BY
            Trainer.id
        LIMIT
            1
    ) AS TOP
WHERE
    SUBQ.levelSum = TOP.levelSum
ORDER BY
    SUBQ.name;