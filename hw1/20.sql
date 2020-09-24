SELECT
    nameId.name,
    idPokeCount.pokeCount
FROM
    (
        SELECT
            Trainer.name,
            Trainer.id
        FROM
            Trainer
        WHERE
            Trainer.hometown = 'Sangnok City'
    ) AS nameId,
    (
        SELECT
            Trainer.id,
            COUNT(*) AS pokeCount
        FROM
            Trainer,
            CatchedPokemon
        WHERE
            Trainer.id = CatchedPokemon.owner_id
        GROUP BY
            Trainer.id
    ) AS idPokeCount
WHERE
    idPokeCount.id IN (
        SELECT
            nameId.id
    )
ORDER BY
    idPokeCount.pokeCount;