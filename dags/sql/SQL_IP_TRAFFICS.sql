-- Last Updated : 2020-11-20
-- Author       : Spyridon Mintzas
-- Description  : Get the traffic from bigflow for high traffic IPs.
-- Comments     :

DECLARE day_1 TIMESTAMP DEFAULT "{0}";
DECLARE day_1_plus_1 TIMESTAMP DEFAULT "{1}";

WITH
  bf_data AS(
    SELECT
       DATE(PARSE_TIMESTAMP("%d/%m/%Y %H:%M", FORMAT_TIMESTAMP("%d/%m/%Y %H:%M",TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(FIRST_SWITCHED), 300) * 300), "UTC"))) AS bf_date,
       TIME(PARSE_TIMESTAMP("%d/%m/%Y %H:%M", FORMAT_TIMESTAMP("%d/%m/%Y %H:%M",TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(FIRST_SWITCHED), 300) * 300), "UTC"))) AS bf_time,
       SKY_SRC_ASN AS asn,
       IF(IPV6_SRC_ADDR = "::", IPV4_SRC_ADDR, IPV6_SRC_ADDR) AS ip,
       SUM(in_bytes*sc.scale) *8*10000 / 300 / 1000000000 AS gbps,

    FROM netflow.bigflow AS nf
    JOIN netflow2.bigflow_scale AS sc
      ON sc.time = TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(first_switched), 300) * 300)
  
    WHERE 1=1
      AND SKY_SRC_ASN IN (SELECT ASN FROM refdata.suspect_asn WHERE Analyse = 1)
      AND _partitiondate IN (DATE(day_1), DATE(day_1_plus_1))
      AND DATE(FIRST_SWITCHED) IN (DATE(day_1))
      AND exporter_ipv4_address IN (SELECT ip FROM refdata.routers where IS_PT)
      AND IF(IPV6_SRC_ADDR = "::", IPV4_SRC_ADDR, IPV6_SRC_ADDR) LIKE "%.%"
    GROUP BY
       bf_date,
       bf_time,
       asn,
       ip)

  ,thresh AS(
    SELECT
       asn,
       ip,
       SUM(gbps) AS gbps_day,
    FROM bf_data
    GROUP BY
       asn,
       ip)
 
  ,bf_data_w_thresh AS(
    SELECT
       bf_date,
       bf_time,
       bf_data.asn,
       bf_data.ip,
       gbps,
       thresh.gbps_day,
    FROM bf_data
    LEFT JOIN thresh
    ON bf_data.asn = thresh.asn
    AND bf_data.ip = thresh.ip)
 
-- Sort and filter IPs with the threshold value of more than 1
SELECT * FROM bf_data_w_thresh
WHERE gbps_day > 1
ORDER BY bf_date ASC ,ip ,bf_time ASC