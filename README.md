# Real Time Fraud Detection System

## Overview
This repository documents the development process and components involved in building a Real-Time Fraud Detection System.

### Motivation
The project was initiated during an internship at Ramana Tech School, where the task of creating a real-time fraud detection system was assigned. The motivation behind this endeavor was to address the growing challenges posed by fraudulent activities in various industries.

### Process
The development process involved several key steps:

- **Data Pipeline Creation**: We established a data pipeline from a Postgresql table, enabling seamless extraction, transformation, and loading of data into our system.
  
- **Kafka Topic Setup**: Kafka topics were configured to facilitate real-time data streaming and communication between components. A producer component subscribed to a topic for publishing data, while a consumer component retrieved and processed the data.
  
- **Data Capture**: The producer component captured data from the source (Postgresql table) and published it to the Kafka topic.

- **Data Analysis**: The consumer component subscribed to the Kafka topic, retrieved the incoming data, and analyzed it using PySpark. PySpark, a powerful big data tool, allowed for efficient analysis of large-scale data.

- **Integration and Insertion**: After analysis, the results were sent to another component responsible for further processing. This component included reporting mechanisms. Finally, the analyzed data was inserted into another Postgresql table for storage.  

### Tools
The following tools were utilized in the development of the fraud detection system:

- **Pyspark**: Efficiently processed and analyzed large volumes of data in a distributed computing environment.
  
- **Kafka**: Facilitated real-time data streaming and communication between system components.
  
- **Zookeeper**: Provided coordination and management capabilities for Kafka, ensuring reliability and fault tolerance.
  
- **Postgresql (Psycopg2)**: Served as the primary data source and destination for storing and retrieving transaction data.
  
- **Redis**: Used for caching and enhancing the performance of specific operations within the system.

## Conclusion
This README provides an overview of the motivation, development process, and tools used in building the Real-Time Fraud Detection System. For more detailed information, refer to the individual components and documentation within the repository.

