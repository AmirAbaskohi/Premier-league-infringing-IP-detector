  -- Last Updated : 2021-08-11
  -- Author       : Greg Mungall
  -- Description  : Run as part of pl_uk_ip_piracy_report_traffic. Gets season
  -- and game week for KO timestamp. KO timestamp inserted programatically by
  -- pl_uk_ip_piracy_report_traffic.py.

  #standardSQL

SELECT
  season,
  game_week
FROM
  `stone-flux-161611.refdata.fixtures`
WHERE
  match_time_utc = '$ko_timestamp'
  AND type = 'PL'
GROUP BY
  season,
  game_week