# BatchProcessingApplication1702281

Download the .jar file in order to run the application

Running application on local Spark Apache cluster:

1. Navigate to your spark directory
2. Place the .jar file and your text file in the directory
3. Run the start-master.sh file, from the spark directory the command should be: 

      ./sbin/start-master.sh
      
4. Open your brower and navigate to http://localhost:8080/ , take note of the spark URL
5. Navigate back to the terminal and run the start-slave.sh file, from the spark directory the command should be: (Make sure to replace <YOUR SPARK URL> with the URL of your Spark instance)

      ./sbin/start-slave.sh <YOUR SPARK URL HERE> 

6. Run spark-submit for the application, from the spark directory the command will be: (Make sure to replace <YOUR SPARK URL> with the URL of your Spark instance)

      ./bin/spark-submit --class "BatchProcessingApplication_1702281" --master <YOUR SPARK URL> BatchProcessingApplication-1.0.jar
  
7. The application will prompt your for the name of the file to process, enter the name of the file (including .txt)
8. Wait for the application to finish running
9. Open your spark directory to find the output file (it will be named "output-" followed by the name of the input file)
10. Repeat steps 6 to 9 for each file you want to process
11. Once you're done run the following command from your Spark folder:

      ./sbin/stop-all.sh

Running application on AWS EMR instance with Spark Apache cluster:

1.
