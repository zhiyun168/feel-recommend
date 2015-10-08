SPARK_DAEMON_JAVA_OPTS+="-Dspark.storage.blockManagerHeartBeatMs=12000000 -Dspark.local.dir=/tmp/spark1,/tmp/spark2"
export SPARK_DAEMON_JAVA_OPTS

dir=`date -d "-4 day" +%Y%m%d`

/home/ubuntu/app/spark-1.4.0-bin-hadoop2.3/bin/spark-submit --master local[3] --executor-memory 5g --driver-memory 3g  --class com.feel.statistics.MergeResultGenderPlatform ../jar/feel-recommend-1.0-SNAPSHOT.jar "../data/$dir/total" "../data/$dir/post_card" "../data/$dir/active_1days_gen_plat" "../data/$dir/active_2days_gen_plat" "../data/$dir/active_3days_gen_plat" "../data/$dir/active_gen_plat" "../data/$dir/nostop_gen_plat"
