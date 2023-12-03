
loop=500
count=1

while [ $count -le $loop ]; do
    date
    echo "start test $count"
    for test_name in TestStaticShards TestJoinLeave TestSnapshot TestMissChange \
                     TestConcurrent1 TestConcurrent2 TestConcurrent3 TestUnreliable1 \
                     TestUnreliable2 TestUnreliable3 TestChallenge1Delete TestChallenge2Unaffected TestChallenge2Partial
    do
        go test -race -run $test_name > log_${test_name}_${count}
        if [ $? -eq 0 ]; then
            rm log_${test_name}_${count}
        else
            echo "Fail $test_name at iter $i, saving log"
        fi
    done
    echo ""
    ((count++))
done