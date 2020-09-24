-- 9
SELECT
    Trainer.name,
    SUBQ.avgLevel
FROM
    Trainer,
    Gym,
    (
        SELECT
            CatchedPokemon.owner_id,
            AVG(CatchedPokemon.level) as avgLevel
        FROM
            CatchedPokemon
        GROUP BY
            CatchedPokemon.owner_id
    ) AS SUBQ
WHERE
    Trainer.id = Gym.leader_id
    AND Trainer.id = SUBQ.owner_id
ORDER BY
    Trainer.name;