  -- Last Updated : 2021-08-10
  -- Author       : Greg Mungall
  -- Description  : Run as part of pl_uk_ip_piracy_report_traffic. Deletes rows
  -- in table with matching KO timestamp. Table and KO timestamp inserted
  -- programatically by pl_uk_ip_piracy_report_traffic.py.

#standardSQL

DELETE
  `$table_id`
WHERE
  ko_timestamp = '$ko_timestamp';
