RACE=-race

if [ -z "$RACE" ]
then
    echo "*** close race detect"
else
    echo "*** open race detect"
fi

rm -rf TEMP-*
rm -rf mr-*

if [[ $1 = "s" ]]
then
    echo "*** start server test"
    (go run $RACE mrcoordinator.go pg-*.txt) || exit 1
elif [[ $1 = "c" ]]
then
    echo "*** build wc.go and start client test"
    (go build $RACE -buildmode=plugin ../mrapps/wc.go) || exit 1
    (go run $RACE mrworker.go wc.so) || exit 1
else
    echo "*** clean output data"
fi