# Mini HDFS Project

This project is a simplified implementation of the Hadoop Distributed File System (HDFS) using Python. It is designed as a mini project to demonstrate core distributed file system concepts such as metadata management, block replication, fault tolerance, and client-server communication.

## Objective

The objective of this project is to simulate the basic architecture and workflow of HDFS by implementing:
- A NameNode for metadata management
- Multiple DataNodes for storing file blocks
- Block replication across DataNodes
- Fault tolerance through replica-based recovery
- A Secondary NameNode for checkpointing
- A Client to interact with the system

This project is intended strictly for academic and learning purposes.

## Project Structure

Src/  
├── client/  
├── datanode_0.py  
├── datanode_1.py  
├── datanode_2.py  
├── namenode.py  
├── secondary_namenode.py  
├── config.py  
└── README.md  

## Components Description

### NameNode

The NameNode acts as the master of the system. It maintains all filesystem metadata including file names, directory structure, block-to-DataNode mappings, and replication information. It monitors DataNode availability and manages block placement and recovery.

### DataNodes

DataNodes store the actual data blocks of files. Each block is replicated across multiple DataNodes as specified by the replication factor. DataNodes respond to read/write requests from clients and periodically report their status to the NameNode.

### Secondary NameNode

The Secondary NameNode periodically performs checkpointing by merging filesystem metadata and edit logs. This helps reduce NameNode recovery time in case of failure. It does not replace the NameNode.

### Client

The client provides an interface for users to interact with the mini HDFS system. It supports operations such as uploading files, reading files, and listing stored files.

## Replication and Fault Tolerance

- Each file is divided into fixed-size blocks.
- Every block is replicated across multiple DataNodes.
- If a DataNode fails, the NameNode detects the failure and redirects read requests to available replicas.
- Replication ensures data availability and reliability in case of node failures.

## Requirements

- Python 3.7 or higher
- Linux or Unix-based operating system recommended
- No external libraries required

## How to Run

### Step 1: Clone the Repository

git clone https://github.com/kaushal1014/18_Project1_BD.git  
cd 18_Project1_BD  

### Step 2: Configure

Edit the config.py file to configure:
- Number of DataNodes
- Replication factor
- Hostnames and ports

### Step 3: Start the NameNode

python3 namenode.py  

### Step 4: Start the DataNodes

Run each DataNode in a separate terminal:

python3 datanode_0.py  
python3 datanode_1.py  
python3 datanode_2.py  

### Step 5: Run the Client

python3 client/client.py  

(The exact commands depend on the client implementation.)

## Working of the System

1. The client sends a request to store a file.
2. The NameNode splits the file into blocks and decides replica placement.
3. DataNodes store block replicas.
4. Metadata is updated in the NameNode.
5. During read operations, the client accesses the nearest available replica.
6. On DataNode failure, the NameNode ensures continued access via replicas.

## Applications

- Understanding HDFS architecture
- Demonstrating replication and fault tolerance
- Academic mini project for Big Data and Distributed Systems courses

## Conclusion

This mini HDFS project demonstrates key features of HDFS including block replication and fault tolerance. It provides practical insight into how distributed storage systems maintain data reliability and availability.

