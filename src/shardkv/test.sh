

trap 'exit 1' INT

echo "Running test $1 for $2 iters"
for i in $(seq 1 $2); do
    echo -ne "\r$i / $2"
    LOG="$1_$i.txt"
    # Failed go test return nonzero exit codes
    if [ "$1" = "all" ]; then
        go test -race &> $LOG
    else
        go test -race -run $1 &> $LOG
    fi
    if [[ $? -eq 0 ]]; then
        rm $LOG
    else
        echo "Failed at iter $i, saving log at $LOG"
    fi
done