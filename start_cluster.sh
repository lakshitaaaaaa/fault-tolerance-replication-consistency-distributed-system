set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$SCRIPT_DIR/app/node.py" ]; then
    NODE="$SCRIPT_DIR/app/node.py"
elif [ -f "$SCRIPT_DIR/src/fault_tolerance_replication_consistency_ds/node.py" ]; then
    NODE="$SCRIPT_DIR/src/fault_tolerance_replication_consistency_ds/node.py"
else
    echo "Could not find node.py. Expected app/node.py or src/fault_tolerance_replication_consistency_ds/node.py." >&2
    exit 1
fi
DATA_DIR="$SCRIPT_DIR/data"
mkdir -p "$DATA_DIR"

python3 "$NODE" --id 1 --port 8001 --peers 2=http://127.0.0.1:8002,3=http://127.0.0.1:8003 --data-dir "$DATA_DIR" & p1=$!
python3 "$NODE" --id 2 --port 8002 --peers 1=http://127.0.0.1:8001,3=http://127.0.0.1:8003 --data-dir "$DATA_DIR" & p2=$!
python3 "$NODE" --id 3 --port 8003 --peers 1=http://127.0.0.1:8001,2=http://127.0.0.1:8002 --data-dir "$DATA_DIR" & p3=$!
trap 'kill "$p1" "$p2" "$p3" 2>/dev/null || true' INT TERM EXIT
wait