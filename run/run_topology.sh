#!/bin/bash 
MESSAGES_PER_SECOND=$1
RUNNING_TIME=$2
LATENCY=$3
STORAGE_FOLDER=$4
PEM_FILE=$5

mkdir -p $STORAGE_FOLDER

ssh -i $PEM_FILE ubuntu@telles-samza-master 'alarm-samza/deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file:/home/ubuntu/alarm-samza/deploy/samza/config/consumptions.properties' &
#deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://home/ubuntu/alarm-samza/deploy/samza/config/averages.properties
sleep 30
case "$MESSAGES_PER_SECOND" in
    "800" )
        nohup ssh ubuntu@tricolor '/bin/bash kafka-producer/produce.sh 1' &;;
    "1800" )
        nohup ssh ubuntu@tricolor '/bin/bash kafka-producer/produce.sh 2' & ;;
    "2500" )
        nohup ssh ubuntu@tricolor '/bin/bash kafka-producer/produce.sh 3' & ;;
    "4200" )
        nohup ssh ubuntu@tricolor '/bin/bash kafka-producer/produce.sh 5' & ;;
    "8000" )
        nohup ssh ubuntu@tricolor '/bin/bash kafka-producer/produce.sh 10' & ;;
    "16800" )
        nohup ssh ubuntu@tricolor '/bin/bash kafka-producer/produce.sh 10' &
        nohup ssh ubuntu@henrique '/bin/bash kafka-producer/produce.sh 10' &
        nohup ssh ubuntu@raildo '/bin/bash kafka-producer/produce.sh 10' & ;;
    "25200" )
        nohup ssh ubuntu@tricolor '/bin/bash kafka-producer/produce.sh 10' &
        nohup ssh ubuntu@henrique '/bin/bash kafka-producer/produce.sh 10' &
        nohup ssh ubuntu@raildo '/bin/bash kafka-producer/produce.sh 10' &
        nohup ssh ubuntu@rodrigo '/bin/bash kafka-producer/produce.sh 10' & ;;
    "33600" )
        nohup ssh ubuntu@tricolor '/bin/bash kafka-producer/produce.sh 30' &
        nohup ssh ubuntu@henrique '/bin/bash kafka-producer/produce.sh 30' &
        nohup ssh ubuntu@raildo '/bin/bash kafka-producer/produce.sh 30' &
        nohup ssh ubuntu@rodrigo '/bin/bash kafka-producer/produce.sh 30' & ;;
esac

sleep "$RUNNING_TIME"m

python clean_up.py 

#Copy files to folder
for i in {1..4}
do
    scp -i $PEM_FILE ubuntu@telles-samza-slave$i:~/output.log  $STORAGE_FOLDER/worker-$i.log
done

for i in {1..4}
do
   if [ "$LATENCY" == "true" ];
   then
       if [ -f $STORAGE_FOLDER/worker-$i.log ];
       then
           python latency_parser.py $STORAGE_FOLDER/worker-$i.log $STORAGE_FOLDER/worker-trimmed-$i.log
       else
           echo "hour;minute;second;latency" > $STORAGE_FOLDER/worker-trimmed-$i.log
       fi
   else
       if [ -f $STORAGE_FOLDER/worker-$i.log ];
       then
           python parser.py $STORAGE_FOLDER/worker-$i.log $STORAGE_FOLDER/worker-trimmed-$i.log
       else
           echo "hour;minute;second;event;total" > $STORAGE_FOLDER/worker-trimmed-$i.log
       fi
   fi
done

#Delete files from remote
ssh -i $PEM_FILE ubuntu@telles-samza-slave1 'sudo rm -rf ~/output.log'
ssh -i $PEM_FILE ubuntu@telles-samza-slave2 'sudo rm -rf ~/output.log'
ssh -i $PEM_FILE ubuntu@telles-samza-slave3 'sudo rm -rf ~/output.log'
ssh -i $PEM_FILE ubuntu@telles-samza-slave4 'sudo rm -rf ~/output.log'

sleep 1m
