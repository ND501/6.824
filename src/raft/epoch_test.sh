#! /bin/bash

test_name=$1
epoch=$2
output="test_log"

echo "run test ${test_name} for epoch ${epoch}"

if [ ! -d "./${output}" ]
then
  mkdir ./${output}
fi

if [ ! -d "./${output}/${test_name}" ]
then
  mkdir ./${output}/${test_name}
fi

for((i = 1; i <= $epoch ;i++))
do
  nohup time go test -run ${test_name} > ./${output}/${test_name}/${i}.log 2>&1 &
  wait
  false_name=$(cat ./${output}/${test_name}/${i}.log | grep 'FAIL' | wc -l)
  wait

  if [ $false_name -ne 0 ]
  then
    echo "iter ${i} fail"
  else
    rm ./${output}/${test_name}/${i}.log
  fi
done