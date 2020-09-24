-- 18
SELECT
    AVG(CatchedPokemon.level)
FROM
    CatchedPokemon
WHERE
    CatchedPokemon.owner_id IN (
        SELECT
            Gym.leader_id
        FROM
            Gym
    );