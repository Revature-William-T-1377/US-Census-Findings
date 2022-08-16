# How to Run this Program:
1. Clone this repository
2. Open the cloned repository with your favorite IDE
3. Build the project with the provided build.sbt file
4. Navigate to src/main/sparkConnector/run.scala and run this program
   - This with scrap all needed data and perform all ETL operations
5. Run src/main/Main.scala
   - This will perform all queries 
   - The output to these queries will be stored in the resultCsv directory
   - By default, this program will read data that is locally stored
   - To enable this program to read data from AWS, you must have access and secret keys and pass in "--cloud" as a command line argument