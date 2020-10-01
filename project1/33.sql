SELECT
    SUM(CatchedPokemon.level)
FROM
    CatchedPokemon,
    Trainer
WHERE
    CatchedPokemon.owner_id = Trainer.id
    AND Trainer.name = 'Matis'
GROUP BY
    Trainer.id