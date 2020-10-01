SELECT
    COUNT(*) AS pokeCount
FROM
    Pokemon,
    CatchedPokemon
WHERE
    Pokemon.id = CatchedPokemon.pid
GROUP BY
    Pokemon.type
ORDER BY
    Pokemon.type;