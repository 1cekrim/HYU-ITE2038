--10
SELECT
    CatchedPokemon.nickname
FROM
    CatchedPokemon
WHERE
    CatchedPokemon.level >= 50
    AND CatchedPokemon.owner_id >= 6
ORDER BY
    CatchedPokemon.nickname;