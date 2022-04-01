# Flink Project
This project was part of the course Cloud Computing and Big Data Ecosystems at Universidad Politécnica de Madrid as part of the Master EIT Digital Data Science. The goal of the project was to develop Flink code to perform analyses on streaming data. The exercises were as follows. The data used was about cars going from east to west or vice versa on different segments.

**Exercise 1** <br>
Retrieve the number of vehicles per hour

**Exercise 2** <br>
Calculate the average distance of the cars that are going to the east side.

**Exercise 3** <br>
Calculate the maximum average speed on each segment

To run this project, undertake the following steps:
1. Install Flink on your machine from the Apache website
2. Install Maven on your laptop. 
3. Package the java file in a .jar file using the command 'mvn clean package'
4. Run the .jar using the command './bin/run -c package.name.flink.class path/to/file.jar -input "file" -output "fileout"', where file is the input data and fileout is the path to the output file.

