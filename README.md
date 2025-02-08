# Chord Protocol Simulation

## Overview
This project implements a **Chord Protocol Simulator** using the **Actor Model** with the **AKKA module** in **F#**. It simulates a peer-to-peer distributed hash table (DHT) system and evaluates the average number of hops for lookup requests.

## How to Run the Program?

### **Prerequisites**
- **Editor:** Visual Studio Code
- **Programming Language:** F#
- **Supported OS:** Windows, MacOS
- **Dependencies:**
  - Install **Ionide for F# Plugin** for Visual Studio Code
  - Install **.NET SDK** from [dotnet.microsoft.com/download](https://dotnet.microsoft.com/download)

### **Setup and Execution**
1. Download and extract the **Team27.zip** file.
2. Open the `program.fs` file in **VSCode**.
3. Open a terminal and navigate to the project folder.
4. Install the **AKKA** package by running:
   ```sh
   dotnet add package Akka.FSharp
    ```
5. Compile the program:
  ```sh
  dotnet build
  ```
If the build is successful, the terminal displays a "Build succeeded" message.  
6. Run the program with the following command:
  ```sh
  dotnet run <number_of_nodes> <number_of_requests>
  ```
Both number_of_nodes and number_of_requests should be integers.  

## Functionality
1. Implements the Chord protocol using AKKA Actor Model.
2. Nodes are sequentially added, forming a distributed hash table (DHT).
3. Each node maintains successors, predecessors, and finger tables.
4. Lookup requests simulate distributed key-value retrieval.
5. The simulator measures the average number of hops per lookup.

## Assumptions
1. The network consists of distributed nodes in a software environment.
2. m = 20 is used to create m-bit identifiers.
3. The network is dynamic, allowing nodes to join and leave.
4. The protocol ensures graceful termination of all nodes.

## Largest Network Tested
Nodes: 1500  
Requests: 10  
Average Hop Count: 11.521933
