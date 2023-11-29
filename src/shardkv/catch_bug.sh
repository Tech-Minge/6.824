
i=0
exit=0
while [ $exit -eq 0 ]; do
    echo "start test $i"
    for test_name in TestStaticShards TestJoinLeave TestSnapshot TestMissChange TestConcurrent1 TestConcurrent2 TestConcurrent3
    do
        echo "start $test_name"
        go test -race -run $test_name > log_$test_name
        if [ $? -eq 0 ]; then
            rm log_$test_name
        else
            echo "Failed at iter $i, saving log"
            exit=1
            break
        fi
    done
    echo ""
    ((i++))
done