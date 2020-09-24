SELECT
    Trainer.name,
    COUNT(*) AS pokeCount
FROM
    Trainer,
    CatchedPokemon
WHERE
    CatchedPokemon.owner_id = Trainer.id
GROUP BY
    Trainer.name,
    CatchedPokemon.pid
HAVING
    pokeCount >= 2;