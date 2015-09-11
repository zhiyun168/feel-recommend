SPARK_DAEMON_JAVA_OPTS+="-Dspark.storage.blockManagerHeartBeatMs=12000000 -Dspark.local.dir=/tmp/spark1,/tmp/spark2"
export SPARK_DAEMON_JAVA_OPTS

dir1=`date -d "-2 day" +%Y%m%d`
dir2=`date -d "-3 day" +%Y%m%d`
dir3=`date -d "-4 day" +%Y%m%d`

/home/ubuntu/app/spark-1.4.0-bin-hadoop2.3/bin/spark-submit --master local[3] --executor-memory 5g --driver-memory 3g  --class com.feel.statistics.ActiveNewUserOf3Days ../jar/feel-recommend-1.0-SNAPSHOT.jar "../data/recently_active_user_file" "../data/1days_ago"  "../data/2days_ago" "../data/3days_ago"  "../data/$dir1/active_1days" "../data/$dir2/active_2days" "../data/$dir3/active_3days" "../data/$dir1/total" "../data/$dir2/total" "../data/$dir3/total"
