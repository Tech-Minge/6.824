
i=0
while ((++i)); do
    echo "start test $i"
    go test -race -run TestSnapshotInstallCrash2D > log
    if [[ $? -eq 0 ]]; then
        rm log
    else
        echo "Failed at iter $i, saving log"
        break
    fi
done