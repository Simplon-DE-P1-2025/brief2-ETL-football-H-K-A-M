# COMPREHENSION ET GENERALITE DES COUPES DU MONDRES

-- 1 Nombre total de matchs disputées par Coupe du Monde
SELECT edition, COUNT(*) AS total_matches
FROM matches_normalized
WHERE is_final = true
GROUP BY edition
ORDER BY total_matches DESC;

-- 2 Nombre total de buts marqués toutes éditions confondues
SELECT SUM(h."Number_of_goals_scored" + a."Number_of_goals_scored") AS total_goals
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
WHERE m.is_final = true;

-- Nombre total de matchs disputés en Coupe du Monde (tournoi final uniquement)
SELECT COUNT(*) AS total_matches_world_cup
FROM matches_normalized
WHERE is_final = true;

-- Moyenne globale des buts par match
SELECT ROUND(AVG(h."Number_of_goals_scored" + a."Number_of_goals_scored"), 2) AS avg_goals_per_match
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
WHERE m.is_final = true;


# ANALYSE PAR EQUIPE

-- Nombre de matchs joués par équipe
SELECT t."Team_name", COUNT(mt.id_match) AS matches_played
FROM (
  SELECT id_match, id_team FROM home_stats

    UNION ALL
      SELECT id_match, id_team FROM away_stats
      ) mt
      JOIN teams_reference t ON mt.id_team = t.id_team
      GROUP BY t."Team_name"
      ORDER BY matches_played DESC

-- Nombre total de victoires par équipe
      SELECT t."Team_name", COUNT(*) AS wins
      FROM matches_normalized m
      JOIN home_stats h ON m.id_match = h.id_match
      JOIN away_stats a ON m.id_match = a.id_match
      JOIN teams_reference t ON (
          (h.id_team = t.id_team AND h."Number_of_goals_scored" > a."Number_of_goals_scored")
           OR (a.id_team = t.id_team AND a."Number_of_goals_scored" > h."Number_of_goals_scored")
           )
           WHERE m.is_final = true
           GROUP BY t."Team_name"
           ORDER BY wins DESC;

 -- Nombre total de défaites par équipe
           SELECT t."Team_name", COUNT(*) AS losses
           FROM matches_normalized m
           JOIN home_stats h ON m.id_match = h.id_match
           JOIN away_stats a ON m.id_match = a.id_match
           JOIN teams_reference t ON (
               (h.id_team = t.id_team AND h."Number_of_goals_scored" < a."Number_of_goals_scored")
                OR (a.id_team = t.id_team AND a."Number_of_goals_scored" < h."Number_of_goals_scored")
                )
                WHERE m.is_final = true
                GROUP BY t."Team_name"
                ORDER BY losses DESC;

  -- Nombre total de matchs nuls par équipe
                SELECT t."Team_name", COUNT(*) AS draws
                FROM matches_normalized m
                JOIN (
                  SELECT id_match, id_team FROM home_stats
                    UNION ALL
                      SELECT id_match, id_team FROM away_stats
                      ) g ON m.id_match = g.id_match
                      JOIN teams_reference t ON g.id_team = t.id_team
                      WHERE m.is_final = true AND m.result = 'draw'
                      GROUP BY t."Team_name"
                      ORDER BY draws DESC;

  -- Pourcentage de réussite (taux de victoire)
                      SELECT t."Team_name",
                             ROUND(SUM(CASE WHEN m.result = t."Team_name" THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS win_rate_pct
                             FROM matches_normalized m
                             JOIN (
                               SELECT id_match, id_team FROM home_stats
                                 UNION ALL
                                   SELECT id_match, id_team FROM away_stats
                                   ) g ON m.id_match = g.id_match
                                   JOIN teams_reference t ON g.id_team = t.id_team
                                   WHERE m.is_final = true
                                   GROUP BY t."Team_name"
                                   ORDER BY win_rate_pct DESC;
# Records et écarts amusants

                                   -- Équipe ayant marqué le plus de buts dans un seul match
                                   SELECT t."Team_name", MAX(g."Number_of_goals_scored") AS max_goals_in_match
                                   FROM (
                                     SELECT id_match, id_team, "Number_of_goals_scored" FROM home_stats
                                       UNION ALL
                                         SELECT id_match, id_team, "Number_of_goals_scored" FROM away_stats
                                         ) g
                                         JOIN teams_reference t ON g.id_team = t.id_team
                                         GROUP BY t."Team_name"
                                         ORDER BY max_goals_in_match DESC
                                         LIMIT 1;



                                         -- Équipe ayant encaissé le plus de buts dans un seul match
                                         SELECT t."Team_name", MAX(g."Number_of_goals_conceded") AS max_goals_conceded
                                         FROM (
                                           SELECT id_match, id_team, "Number_of_goals_conceded" FROM home_stats
                                             UNION ALL
                                               SELECT id_match, id_team, "Number_of_goals_conceded" FROM away_stats
                                               ) g
                                               JOIN teams_reference t ON g.id_team = t.id_team
                                               GROUP BY t."Team_name"
                                               ORDER BY max_goals_conceded DESC
                                               LIMIT 1;

                                               -- Équipes invaincues sur une édition
                                               SELECT edition, t."Team_name"
                                               FROM matches_normalized m
                                               JOIN (
                                                 SELECT id_match, id_team FROM home_stats
                                                   UNION ALL
                                                     SELECT id_match, id_team FROM away_stats
                                                     ) g ON m.id_match = g.id_match
                                                     JOIN teams_reference t ON g.id_team = t.id_team
                                                     WHERE m.is_final = true
                                                     GROUP BY edition, t."Team_name"
                                                     HAVING SUM(CASE WHEN m.result = t."Team_name" THEN 1 ELSE 0 END) = COUNT(*);

                                                     -- Équipes ayant perdu le plus de finales
                                                     SELECT t."Team_name", COUNT(*) AS lost_finals
                                                     FROM matches_normalized m
                                                     JOIN (
                                                       SELECT id_match, id_team FROM home_stats
                                                         UNION ALL
                                                           SELECT id_match, id_team FROM away_stats
                                                           ) g ON m.id_match = g.id_match
                                                           JOIN teams_reference t ON g.id_team = t.id_team
                                                           WHERE m.round ILIKE '%final%' AND m.is_final = true AND m.result != t."Team_name"
                                                           GROUP BY t."Team_name"
                                                           ORDER BY lost_finals DESC;

                                                           -- Équipes qui se sont le plus affrontées
                                                           SELECT t1."Team_name" AS team_1, t2."Team_name" AS team_2, COUNT(*) AS matches
                                                           FROM home_stats h
                                                           JOIN away_stats a ON h.id_match = a.id_match
                                                           JOIN teams_reference t1 ON h.id_team = t1.id_team
                                                           JOIN teams_reference t2 ON a.id_team = t2.id_team
                                                           GROUP BY team_1, team_2
                                                           ORDER BY matches DESC
                                                           LIMIT 10;


                                                           # ANALYSE GEOGRAPHIQUE

                                                           -- Top villes ayant accueilli le plus de matchs
                                                           SELECT city, COUNT(*) AS matches
                                                           FROM matches_normalized
                                                           WHERE is_final = true
                                                           GROUP BY city
                                                           ORDER BY matches DESC;

                                                           -- Répartition des matchs par continent (si mapping ville → continent disponible)
                                                           -- Exemple : suppose une table "cities" avec city → continent
                                                           SELECT c.continent, COUNT(m.id_match) AS matches
                                                           FROM matches_normalized m
                                                           JOIN cities c ON m.city = c.city
                                                           WHERE m.is_final = true
                                                           GROUP BY c.continent
                                                           ORDER BY matches DESC;


                                                           # ANALYSES TEMPORELLES

                                                           -- Évolution du nombre moyen de buts par match au fil des éditions
                                                           SELECT edition, ROUND(AVG(h."Number_of_goals_scored" + a."Number_of_goals_scored"), 2) AS avg_goals
                                                           FROM matches_normalized m
                                                           JOIN home_stats h ON m.id_match = h.id_match
                                                           JOIN away_stats a ON m.id_match = a.id_match
                                                           WHERE m.is_final = true
                                                           GROUP BY edition
                                                           ORDER BY edition;

                                                           -- Évolution du taux de matchs nuls par édition
                                                           SELECT edition,
                                                                  ROUND(SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS draw_rate_pct
                                                                  FROM matches_normalized
                                                                  WHERE is_final = true
                                                                  GROUP BY edition
                                                                  ORDER BY edition;

  UNION ALL
  SELECT id_match, id_team FROM away_stats
) mt
JOIN teams_reference t ON mt.id_team = t.id_team
GROUP BY t."Team_name"
ORDER BY matches_played DESC;

-- Nombre total de victoires par équipe
SELECT t."Team_name", COUNT(*) AS wins
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
JOIN teams_reference t ON (
    (h.id_team = t.id_team AND h."Number_of_goals_scored" > a."Number_of_goals_scored")
 OR (a.id_team = t.id_team AND a."Number_of_goals_scored" > h."Number_of_goals_scored")
)
WHERE m.is_final = true
GROUP BY t."Team_name"
ORDER BY wins DESC;

-- Nombre total de défaites par équipe
SELECT t."Team_name", COUNT(*) AS losses
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
JOIN teams_reference t ON (
    (h.id_team = t.id_team AND h."Number_of_goals_scored" < a."Number_of_goals_scored")
 OR (a.id_team = t.id_team AND a."Number_of_goals_scored" < h."Number_of_goals_scored")
)
WHERE m.is_final = true
GROUP BY t."Team_name"
ORDER BY losses DESC;

-- Nombre total de matchs nuls par équipe
SELECT t."Team_name", COUNT(*) AS draws
FROM matches_normalized m
JOIN (
  SELECT id_match, id_team FROM home_stats
  UNION ALL
  SELECT id_match, id_team FROM away_stats
) g ON m.id_match = g.id_match
JOIN teams_reference t ON g.id_team = t.id_team
WHERE m.is_final = true AND m.result = 'draw'
GROUP BY t."Team_name"
ORDER BY draws DESC;

-- Pourcentage de réussite (taux de victoire)
SELECT t."Team_name",
       ROUND(SUM(CASE WHEN m.result = t."Team_name" THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS win_rate_pct
FROM matches_normalized m
JOIN (
  SELECT id_match, id_team FROM home_stats
  UNION ALL
  SELECT id_match, id_team FROM away_stats
) g ON m.id_match = g.id_match
JOIN teams_reference t ON g.id_team = t.id_team
WHERE m.is_final = true
GROUP BY t."Team_name"
ORDER BY win_rate_pct DESC;


# Records et écarts amusants

-- Équipe ayant marqué le plus de buts dans un seul match
SELECT t."Team_name", MAX(g."Number_of_goals_scored") AS max_goals_in_match
FROM (
  SELECT id_match, id_team, "Number_of_goals_scored" FROM home_stats
  UNION ALL
  SELECT id_match, id_team, "Number_of_goals_scored" FROM away_stats
) g
JOIN teams_reference t ON g.id_team = t.id_team
GROUP BY t."Team_name"
ORDER BY max_goals_in_match DESC
LIMIT 1;

-- Équipe ayant encaissé le plus de buts dans un seul match
SELECT t."Team_name", MAX(g."Number_of_goals_conceded") AS max_goals_conceded
FROM (
  SELECT id_match, id_team, "Number_of_goals_conceded" FROM home_stats
  UNION ALL
  SELECT id_match, id_team, "Number_of_goals_conceded" FROM away_stats
) g
JOIN teams_reference t ON g.id_team = t.id_team
GROUP BY t."Team_name"
ORDER BY max_goals_conceded DESC
LIMIT 1;

-- Équipes invaincues sur une édition
SELECT edition, t."Team_name"
FROM matches_normalized m
JOIN (
  SELECT id_match, id_team FROM home_stats
  UNION ALL
  SELECT id_match, id_team FROM away_stats
) g ON m.id_match = g.id_match
JOIN teams_reference t ON g.id_team = t.id_team
WHERE m.is_final = true
GROUP BY edition, t."Team_name"
HAVING SUM(CASE WHEN m.result = t."Team_name" THEN 1 ELSE 0 END) = COUNT(*);

-- Équipes ayant perdu le plus de finales
SELECT t."Team_name", COUNT(*) AS lost_finals
FROM matches_normalized m
JOIN (
  SELECT id_match, id_team FROM home_stats
  UNION ALL
  SELECT id_match, id_team FROM away_stats
) g ON m.id_match = g.id_match
JOIN teams_reference t ON g.id_team = t.id_team
WHERE m.round ILIKE '%final%' AND m.is_final = true AND m.result != t."Team_name"
GROUP BY t."Team_name"
ORDER BY lost_finals DESC;

-- Équipes qui se sont le plus affrontées
SELECT t1."Team_name" AS team_1, t2."Team_name" AS team_2, COUNT(*) AS matches
FROM home_stats h
JOIN away_stats a ON h.id_match = a.id_match
JOIN teams_reference t1 ON h.id_team = t1.id_team
JOIN teams_reference t2 ON a.id_team = t2.id_team
GROUP BY team_1, team_2
ORDER BY matches DESC
LIMIT 10;


# ANALYSE GEOGRAPHIQUE

-- Top villes ayant accueilli le plus de matchs
SELECT city, COUNT(*) AS matches
FROM matches_normalized
WHERE is_final = true
GROUP BY city
ORDER BY matches DESC;

-- Répartition des matchs par continent (si mapping ville → continent disponible)
-- Exemple : suppose une table "cities" avec city → continent
SELECT c.continent, COUNT(m.id_match) AS matches
FROM matches_normalized m
JOIN cities c ON m.city = c.city
WHERE m.is_final = true
GROUP BY c.continent
ORDER BY matches DESC;


# ANALYSES TEMPORELLES

-- Évolution du nombre moyen de buts par match au fil des éditions
SELECT edition, ROUND(AVG(h."Number_of_goals_scored" + a."Number_of_goals_scored"), 2) AS avg_goals
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
WHERE m.is_final = true
GROUP BY edition
ORDER BY edition;

-- Évolution du taux de matchs nuls par édition
SELECT edition,
       ROUND(SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS draw_rate_pct
FROM matches_normalized
WHERE is_final = true
GROUP BY edition
ORDER BY edition;
