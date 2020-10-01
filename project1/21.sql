SELECT
    gymNameId.name,
    idPokeCount.pokeCount
FROM
    (
        SELECT
            Trainer.name,
            Gym.leader_id
        FROM
            Gym,
            Trainer
        WHERE
            Gym.leader_id = Trainer.id
    ) AS gymNameId,
    (
        SELECT
            Trainer.id,
            COUNT(*) AS pokeCount
        FROM
            Trainer,
            CatchedPokemon
        WHERE
            Trainer.id = CatchedPokemon.owner_id
        GROUP BY
            Trainer.id
    ) AS idPokeCount
WHERE
    gymNameId.leader_id IN (
        SELECT
            idPokeCount.id
    )
ORDER BY
    gymNameId.name;