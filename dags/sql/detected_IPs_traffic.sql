  -- Last Updated : 2021-08-10
  -- Author       : Greg Mungall
  -- Description  : Run as part of pl_uk_ip_piracy_report_traffic. Gets traffic
  -- from bigflow for a list of IPs between two dates. IPs and dates inserted
  -- programatically by pl_uk_ip_piracy_report_traffic.py.

#standardSQL

SELECT
  -- Floor timestamps to 5 min intervals i.e. 00:00:00 -> 00:04:59 = 00:00:00
  TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(first_switched), 300) * 300) AS timestamp,
  ipv4_src_addr AS ip,
  sky_src_asn AS asn,
  Name AS as_name,
  IF ((is_pl2020 = false) AND (Analyse = 1), 2, Analyse) AS analyse,
  COALESCE(is_vpn_datacenter, false) AS vpn,
  vpn_service_name AS vpn_name,
  SUM(in_bytes
    * 10000 -- scale to full sample size
    * scale -- scale to all isps
    * 8 -- convert bytes to bits
    / 1e+9) -- convert to gigabits
    AS gigabits
FROM
  netflow.bigflow
JOIN
  netflow2.bigflow_scale
ON
  TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(first_switched), 300) * 300) = time
LEFT JOIN
  refdata.suspect_asn
ON
  sky_src_asn = asn
LEFT JOIN
  refdata.vpn_ips
ON
  ipv4_src_addr = ip
WHERE
  exporter_ipv4_address IN (SELECT ip FROM refdata.routers WHERE IS_PT)
  AND _partitiontime >= TIMESTAMP_TRUNC(TIMESTAMP '$analysis_start', DAY)
  AND _partitiontime <= TIMESTAMP_TRUNC(TIMESTAMP '$analysis_end', DAY)
  AND FIRST_SWITCHED >= TIMESTAMP '$analysis_start'
  AND FIRST_SWITCHED < TIMESTAMP '$analysis_end'
  AND ipv4_src_addr IN $ip_tuple
GROUP BY
  timestamp,
  ip,
  asn,
  as_name,
  analyse,
  vpn,
  vpn_name
ORDER BY
  timestamp ASC