SPARK_DAEMON_JAVA_OPTS+="-Dspark.storage.blockManagerHeartBeatMs=12000000 -Dspark.local.dir=/tmp/spark1,/tmp/spark2"
export SPARK_DAEMON_JAVA_OPTS

dir=`date -d "-1 day" +%Y%m%d`

/home/ubuntu/app/spark-1.4.0-bin-hadoop2.3/bin/spark-submit --master local[3] --executor-memory 5g --driver-memory 3g  --class com.feel.statistics.NewUserCard ../jar/feel-recommend-1.0-SNAPSHOT.jar  "../data/get_usercard_data" "../data/test_get_data"  "../data/$dir/gender_card" "../data/$dir/platform_card" "../data/$dir/post_card"
