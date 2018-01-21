-- Given a blue_line_key and measure, return local watershed code
SELECT local_watershed_code
FROM whse_basemapping.fwa_stream_networks_sp
WHERE blue_line_key = %s AND downstream_route_measure - .0001 <= %s
ORDER BY downstream_route_measure desc
LIMIT 1