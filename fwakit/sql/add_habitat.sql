UPDATE $InputTable
SET fish_habitat =
  (SELECT fish_habitat
     FROM $FishHabitatTable
    WHERE blue_line_key = %s
      AND downstream_route_measure < %s
 ORDER BY downstream_route_measure DESC
    LIMIT 1)
WHERE $PrimaryKey = %s