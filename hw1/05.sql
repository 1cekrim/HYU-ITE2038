-- 5
SELECT
    Trainer.name
FROM
    Trainer
WHERE
    Trainer.id NOT IN (
        SELECT
            Gym.leader_id
        FROM
            Gym
        WHERE
            Gym.leader_id IS NOT NULL
    )
ORDER BY
    Trainer.name;