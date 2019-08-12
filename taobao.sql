./neo4j-import --into /home/wkq/neo4j/neo4j-community-3.1.0/data/databases/test_10000000_graph.db  --nodes /home/wkq/neo4j/node.csv  --relationships /home/wkq/neo4j/relathionship.csv --trim-strings true --input-encoding UTF-8 --id-type INTEGER --stacktrace true --bad-tolerance 0 --skip-bad-relationships true --skip-duplicate-nodes false


create external table aggregate(
date string,
game_size string,
match_id string,
match_mode string,
party_size string,
player_assists string,
player_dbno string,
player_dist_ride string,
player_dist_walk string,
player_dmg string,
player_kills string,
player_name string,
player_survive_time string,
team_id string,
team_placement string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location "/pubg/aggregate/"
tblproperties(
"skip.header.line.count"="1"
);

create table aggregate_p stored as parquet as select * from aggregate

create external table deaths(
killed_by string,
killer_name string,
killer_placement string,
killer_position_x string,
killer_position_y string,
map0 string,
match_id string,
time string,
victim_name string,
victim_placement string,
victim_position_x string,
victim_position_y string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location "/pubg/deaths/"
tblproperties(
"skip.header.line.count"="1"
);

create table deaths_p stored as parquet as select * from deaths;

create table deaths_t ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' as select * from deaths limit 1000;

create table aggregate_t as select * from aggregate limit 1000;

create external table userBehavior(
user_id string,
shop_id string,
cate_id string,
behavior string,
time string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location "/taobao/";

create table userb stored as parquet as select * from userBehavior;

spark-submit \
--class SimpleApp \
--num-executors 50 \
--executor-memory 6G \
--executor-cores 4 \
--driver-memory 1G \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.3 \

spark-submit \
--class SimpleApp1 \
--master yarn-cluster \
--num-executors 8 \
--executor-memory 1G \
--executor-cores 2 \
--driver-memory 1G \
--conf spark.default.parallelism=36 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.3 \
