-- 8
SELECT
    AVG(CatchedPokemon.level)
FROM
    CatchedPokemon
WHERE
    CatchedPokemon.owner_id = (
        SELECT
            Trainer.id
        FROM
            Trainer
        WHERE
            Trainer.name = 'Red'
    );