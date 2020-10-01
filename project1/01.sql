SELECT
    Trainer.name
FROM
    Trainer,
    CatchedPokemon
WHERE
    Trainer.id = CatchedPokemon.owner_id
GROUP BY
    Trainer.id
HAVING
    COUNT(*) >= 3
ORDER BY
    COUNT(*) desc;