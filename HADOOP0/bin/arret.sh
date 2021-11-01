
NameNode="candy"
NBServer="5"
declare -a HDFSServer=( "newton" "albator" "esteban" "dragon" "carbone")

#stop running servers 
for (( i=0; i<$NBServer; i++ )); do
    ssh ${HDFSServer[$i]} "pkill -f java.*HdfsServeur*"
done
#stop running workers 
for (( i=0; i<$NBServer; i++ )); do
    ssh ${HDFSServer[$i]} "pkill -f java.*WorkerImpl*"
done
# stop the NameNode
ssh ${NameNode} "pkill -f java.*NameNode*"
