SELECT
    Pokemon.type,
    COUNT(*) AS pokeCount
FROM
    Pokemon
GROUP BY
    Pokemon.type
ORDER BY
    pokeCount, Pokemon.type;