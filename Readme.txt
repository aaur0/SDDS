Instructions:
-------------

1. Untar the source and the script files in the machine where the deduplication service is going to run.

2. Make sure python interpreter and the latest jdk are installed on the systems which are going run the deduplication service and the Cassandra       	 datastore.

3. Make sure that your JAVA_HOME points to the latest jdk.

4. Install Cassandra on all the machines that would be part of the cluster. The instructions to install on a Ubuntu box is given in scripts/	     	cassandra_lin.sh. 

5. Install pycassa on all the machines using the instructions given in scripts/pycassa.txt. Pycassa acts as the python client library for Cassandra.

6. Edit cassandra.yaml in the conf directory of Cassandra and change the listen address for intra-cluster communication and the RPC address through   	 which the clients can connect. Also, change the address of the seed node.

7. Start the Cassandra service on all machines in the cluster.

8. Create the database schema on one of the nodes as given in scripts/schema.txt. The schema will get propagated to all the nodes in the cluster.

9. Using the CLI
	a. Put Command: python chunker.py -f -s <file_name>
	   This will fetch the file(Snapshot or the VM Image) and pass it on to the deduplication service. It takes the name of file as parameter. 

	b. Get Command: python chunker.py -f -g <file_name>
	   This service will be used to retrieve the file stored in the database. It would fetch all the individual chunks and stitch them as they 		   appeared in the original file. The file will then be passed on by the consumer of the service. It takes the name of file as parameter.

	c. Metrics: python chunker.py -t
	   This will calculate the total space saved for all the files stored our system.
