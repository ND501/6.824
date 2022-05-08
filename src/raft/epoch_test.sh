#! /bin/bash
test_name="TestManyElections2A"
epoch=100
output="test_log"

echo "run test ${test_name} for epoch 100"

fail_array=()

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
    #echo $false_name

    if [ $false_name -ne 0 ]
    then
        echo "iter ${i} fail"

    fi
done