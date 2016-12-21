#!/bin/sh

export JAVA_HOME=/usr/local/java

#day_ago_index=$1
#today=`date -d "$day_ago_index day ago" "+%Y-%m-%d"`
#today=`date -d "1 day ago" "+%Y-%m-%d"`

hadoop_dir="/usr/local/hadoop"
hadoop_tool="${hadoop_dir}/bin/hadoop"
hadoop_stream="/letv/search/hadoop-1.1.2/contrib/streaming/hadoop-streaming-1.1.2.jar"

function clear_job()
{
	local job_output=$1
	${hadoop_tool} fs -rm -r -skipTrash ${job_output}
}


#-------------------------------
# -D mapred.reduce.tasks=2 \
# mapred.output.compress=true \
#
#-------------------------------
function run_job()
{
	local mapper=$1
	local reducer=$2
	local job_input=$3
	local job_output=$4
	local task_num=$5
	${hadoop_tool} jar ${hadoop_stream} \
	-jobconf mapred.reduce.tasks=$5 \
	-mapper ${mapper} \
	-reducer ${reducer} \
	-file ${mapper} \
	-file ${reducer} \
	-input ${job_input}/* \
	-output ${job_output}
}

function calcute_related_querys() {
	local mapper="related_query_mapper.py"
	local reducer="related_query_reducer.py"
	local job_input=$1
	local job_output=$2
	local task_num=$3
	clear_job $job_output
	run_job $mapper $reducer $job_input $job_output $task_num
}

function calcute_user_querys() {
	local mapper="user_query_mapper.py"
	local reducer="user_query_reducer.py"
	local job_input=$1
	local job_output=$2
	local task_num=$3
	clear_job $job_output
	run_job $mapper $reducer $job_input $job_output $task_num
}

function main() {
	local day_ago_index=$1
	local today=`date -d "$day_ago_index day ago" "+%Y-%m-%d"`
	echo ${today}
	local job_input="/user/search/logs/search-log/$today"
	local job_ip_query="/user/search/results/related_query_stat/ip_query"
	local job_output="/user/search/results/related_query_stat/$today"
	calcute_user_querys $job_input $job_ip_query 16
	calcute_related_querys $job_ip_query $job_output 1
	clear_job $job_ip_query
}

main $1

