SELECT
    City.name,
    CatchedPokemon.nickname
FROM
    City
    LEFT OUTER JOIN (
        SELECT
            Trainer.hometown,
            MAX(CatchedPokemon.level) as maxLevel
        FROM
            CatchedPokemon,
            Trainer
        WHERE
            Trainer.id = CatchedPokemon.owner_id
        GROUP BY
            Trainer.hometown
    ) AS SUBQ ON City.name = SUBQ.hometown
    LEFT OUTER JOIN CatchedPokemon ON CatchedPokemon.level = SUBQ.maxLevel
    LEFT OUTER JOIN Trainer ON Trainer.hometown = City.name
    AND CatchedPokemon.owner_id = Trainer.id
ORDER BY
    City.name;