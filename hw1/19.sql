-- 19
SELECT
    COUNT(*) AS typeCount
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    CatchedPokemon.owner_id,
                    Pokemon.type
                FROM
                    Pokemon,
                    CatchedPokemon
                WHERE
                    CatchedPokemon.pid = Pokemon.id
            ) AS catchedType
        WHERE
            catchedType.owner_id IN (
                SELECT
                    Gym.leader_id
                from
                    Gym
                WHERE
                    Gym.city = 'Sangnok City'
            )
        GROUP BY
            catchedType.type
    ) AS SUBQ;