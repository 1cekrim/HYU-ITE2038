SELECT
    Trainer.name
FROM
    (
        SELECT
            Evolution.after_id
        FROM
            Evolution
        WHERE
            Evolution.after_id NOT IN (
                SELECT
                    Evolution.before_id
                FROM
                    Evolution
            )
        GROUP BY Evolution.after_id
    ) AS EV,
    Trainer,
    CatchedPokemon
WHERE
    Trainer.id = CatchedPokemon.owner_id
    AND CatchedPokemon.pid = EV.after_id
ORDER BY
    Trainer.name;