
i=0
while ((++i)); do
    echo "start test $i"
    go test -race -run TestPersistPartitionUnreliableLinearizable3A > log
    if [[ $? -eq 0 ]]; then
        rm log
    else
        echo "Failed at iter $i, saving log"
        break
    fi
done