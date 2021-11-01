
# NameNode Address
NameNode="candy"
 
# HDFS Client Address
HDFSClient="zia"

# Directory
DIR="/home/mwissad/2A/Hidoop/hidoop/src"


# HDFS Server Port
HDFSServerPort="8041"
# Number of Server
NBServer="5"

# Worker and Server Address
declare -a Worker=( "newton" "albator" "esteban" "dragon" "carbone")
declare -a HDFSServer=( "newton" "albator" "esteban" "dragon" "carbone")

# Worker Java Directory
WorkerDIR="/home/mwissad/2A/Hidoop/hidoop/src"
# Worker Server Port
WorkerPort="8021"

# Clean log file
rm nohup.out

nohup ssh ${NameNode} "cd ${DIR} && java hdfs.NameNode" & 

# Connection to HDFS Servers
for (( i=0; i<$NBServer; i++ )); do
    nohup ssh ${HDFSServer[$i]}  "cd ${DIR} && java hdfs.HdfsServeur $((HDFSServerPort + $i))" &
    nohup ssh ${Worker[$i]} "cd ${DIR} && java ordo.WorkerImpl $((WorkerPort + $i)) $i "&
done

sleep 4
echo "........."

# Launch HDFS Client
#nohup ssh ${HDFSClient} "cd ${DIR} && java hdfs.HdfsClient write line /home/mwissad/data/filesample.txt && sleep 3 && java application.MyMapReduce /home/mwissad/data/filesample.txt "
nohup ssh ${HDFSClient} "cd ${DIR} && java application.QuasiMonteCarlo 100"
#tail -f nohup.out
