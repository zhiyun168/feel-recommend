SPARK_DAEMON_JAVA_OPTS+="-Dspark.storage.blockManagerHeartBeatMs=12000000 -Dspark.local.dir=/tmp/spark1,/tmp/spark2"
export SPARK_DAEMON_JAVA_OPTS

t=$1
dir=`date -d "-$1 day" +%Y%m%d`
mkdir ../data/$dir

/home/ubuntu/app/spark-1.4.0-bin-hadoop2.3/bin/spark-submit --master local[3] --executor-memory 5g --driver-memory 3g  --class com.feel.statistics.NewUserRegister ../jar/feel-recommend-1.0-SNAPSHOT.jar  "../data/test_get_data" "../data/$dir/total" "../data/$dir/gender" "../data/$dir/platform"
