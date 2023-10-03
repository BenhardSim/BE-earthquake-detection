# BE-earthquake-detection 
Created By : **Benhard Sim**

this is a Back-end application for Earthquake detection data **pipeline** that utilise confluent Kafka for data broker and seedLink protocol with geofon service to predict the existence of earthquake in the Java island region. 

this application also utilize the Machine Learning Model that was trained with P-wave algorithm and data from geofon network with a frequency of 20 Hz. 

this project was created for my bachlore thesis final project in Diponegoro University.

## HOW TO PRODUCE DATA
1. Make sure to install all the dependency
2. Make sure you have the config_kafka.txt file for Kafka access
3. Run this code below to produce the station data in pararel
   ```
   python Producer_pararel/producer_pararel_v2.py
   ```
4. the program will start to produce the data

## HOW TO SEE THE CONSUMED DATA
1. Make sure to install all the dependency
2. Make sure you have the config_kafka.txt file for Kafka access
3. Make sure you have the machine learning model file and replace the ML file path in the wave_consumer_pararel_to_db.py file
4. Make sure you have the Firebase credentials.json file to push the consumed data into the database 
5. Run this code below to consume the station data in pararel 
   ```
   python Wave_consumer.py
   ```
6. the program will start to produce the data

## HOW TO ACCESS THE DATA IN FIREBASE WITH WEBSOCKET
1. go to the Back-end folder
2. install all of the node.js depedency
3. make sure you have the Firebase credentials.json file to access the data from the firebase database
4. Run this code below to the stasiun data in pararel 
   ```
   node Back-end/server.js
   ``` 
5. to check if the data is conneceted go to your web browser and go to localhost with port 3000
