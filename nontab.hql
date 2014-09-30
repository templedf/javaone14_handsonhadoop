create external table logins (
 id string, state string, time string, day string
)
row format serde 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
with serdeproperties (
  "input.regex" =
"(\\d+) in ([A-Z]{2}) at (\\d{2}:\\d{2}:\\d{2}) on (\\d{2}/\\d{2}/\\d{2})"
)
location '/user/cloudera/javaone/logins';
