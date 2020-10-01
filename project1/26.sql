SELECT
    Pokemon.name
FROM
    Pokemon,
    CatchedPokemon
WHERE
    CatchedPokemon.nickname LIKE '% %'
    AND CatchedPokemon.pid = Pokemon.id
ORDER BY
    Pokemon.name desc;