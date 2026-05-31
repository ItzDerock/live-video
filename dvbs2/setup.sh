set -o pipefail

DEVICE=10.60.70.2
CALL=AERO
ROOT="cmd/pluto/$CALL"

## INIT

mosquitto_pub -h $DEVICE -t $ROOT/tx/frequency      -m 2395000000
mosquitto_pub -h $DEVICE -t $ROOT/tx/gain           -m 0 # -20 for PA
mosquitto_pub -h $DEVICE -t $ROOT/tx/mute           -m 0
mosquitto_pub -h $DEVICE -t $ROOT/tx/dvbs2/sr       -m 3000000
mosquitto_pub -h $DEVICE -t $ROOT/tx/dvbs2/fec      -m 3/4
mosquitto_pub -h $DEVICE -t $ROOT/tx/dvbs2/constel  -m qpsk
mosquitto_pub -h $DEVICE -t $ROOT/tx/dvbs2/frame    -m short
mosquitto_pub -h $DEVICE -t $ROOT/tx/dvbs2/pilots   -m 1
mosquitto_pub -h $DEVICE -t $ROOT/tx/dvbs2/fecmode  -m fixed
mosquitto_pub -h $DEVICE -t $ROOT/tx/stream/mode    -m dvbs2-ts

mosquitto_pub -h $DEVICE -t $ROOT/tx/dvbs2/tssourcemode    -m 0
mosquitto_pub -h $DEVICE -t $ROOT/tx/dvbs2/tssourceaddress -m 10.60.70.2:10000
