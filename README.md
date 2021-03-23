# BatchProcessingApplication1702281

Download the .jar file in order to run the application

Running application on local Spark Apache cluster:

1. Navigate to your spark directory
2. Place the .jar file and your text file in the directory
3. Run the start-master.sh file, from the spark directory the command should be: 

      	./sbin/start-master.sh
      
4. Open your brower and navigate to http://localhost:8080/ , take note of the spark URL
5. Navigate back to the terminal and run the start-slave.sh file, from the spark directory the command should be: (Make sure to replace YOUR_SPARK_URL with the URL of your Spark instance)

      	./sbin/start-slave.sh YOUR_SPARK_URL 

6. Run spark-submit for the application, from the spark directory the command will be: (Make sure to replace YOUR_SPARK_URL with the URL of your Spark instance)

      	./bin/spark-submit --class "BatchProcessingApplication_1702281" --master YOUR_SPARK_URL BatchProcessingApplication-1.0.jar
  
7. The application will prompt your for the name of the file to process, enter the name of the file (including .txt)
8. Wait for the application to finish running
9. Open your spark directory to find the output file (it will be named "output-" followed by the name of the input file)
10. Repeat steps 6 to 9 for each file you want to process
11. Once you're done run the following command from your Spark folder:

      	./sbin/stop-all.sh

Running application on AWS EMR instance with Spark Apache cluster:

1. Open the AWS Management Console
2. Navigate to the S3 Bucket Management Console
3. Create a new bucket with Bucket Versioning set to Enabled (Make sure to take note of the name you assign it)
4. Upload the .jar file and any text files you wish to process to the bucket
5. Navigate to the EMR Management Console
6. Create a new cluster with the following configuration:

	for S3 folder, select the S3 Bucket you just created
	
	for Software Configuration make sure to select Spark
	
	for EC2 key pair select Proceed without an EC2 key pair
	
7. Wait for the cluster's status to be Waiting
8. Navigate to the Steps section and selected Add Step with the following configuration:
	
	for Step Type, select Spark application
	
	for Name, type: 
	
		Batch Processing Application
		
	for Deploy mode, select Cluster
	
	for Spark-submit options, type:
	
		--class "BatchProcessingApplication_1702281"
		
	for Application Location, navigate to your S3 Bucket and select the .jar file
	
	for Arguments, type: (Where YOUR_BUCKET_NAME is the name of your bucket and YOUR_TEXT_FILE_NAME is the name of the file you wish to process)
	
		YOUR_BUCKET_NAME YOUR_TEXT_FILE_NAME
		
9. Wait until the status of the step is Completed
10. Navigate to the S3 Bucket Management Console
11. The output file will be in the bucket under the name "output-" followed by the name of the input file
12. If you want to process more than one file, Navigate back to the Steps section of your EMR cluster, select the previously run step and click the Clone step button. Then simply change the arguemnt of the previous text file to whatever text file you wish to process next. 
13. When finished, navigate to the EMR Management Console
14. Select your Cluster and click Terminate
