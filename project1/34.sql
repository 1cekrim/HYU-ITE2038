SELECT
    Pokemon.name,
    CatchedPokemon.level,
    CatchedPokemon.nickname
FROM
    Pokemon,
    Gym,
    CatchedPokemon
WHERE
    CatchedPokemon.pid = Pokemon.id
    AND CatchedPokemon.owner_id in (
        SELECT
            Gym.leader_id
    )
    AND CatchedPokemon.nickname LIKE 'A%'
ORDER BY
    Pokemon.name desc;