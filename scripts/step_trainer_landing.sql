CREATE EXTERNAL TABLE IF NOT EXISTS `hoangnth`.`step_trainer_landing` (
  `sensorreadingtime` string,
  `serialnumber` string,
  `distancefromobject` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-human-balance-bucket/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');