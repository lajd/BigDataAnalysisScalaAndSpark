# Big Data Analysis with Scala and Spark

## Overview

The Big Data Analysis with Scala and Spark course is taught by Prof. Heather Miller, with learning outcomes of:

- Read data from persistent storage and load it into Apache Spark,
- Manipulate data with Spark and Scala,
- Express algorithms for data analysis in a functional style,
- Recognize how to avoid shuffles and recomputation in Spark,

For details, see the course home page: https://www.coursera.org/learn/scala-parallel-programming/home/info.

This is course 3 of 5 in the Functional Programming in Scala Specialization (see here: https://www.coursera.org/specializations/scala).

## Requirements
SBT
Scala >= 3

## Course Contents

#### Week 1: Spark basics
- Data parallel to distributed data parallel
- Latency
- Spark RDDs:
  - Spark's distributed collection
  - Transformations and actions
  - Evaluation in Spark
  - Cluster topology
- Assignment: Wikipedia article popularity

#### Week 2: Reduction operations & distributed key-value pairs
- Reduction operations
- Pair RDDs
- Transformations and actions on pair RDDs
- Joins
- Assignment: distributed KMeans over StackOverflow posts 

#### Week 3: Partitioning and Shuffling
- Shuffling: What is it any why it's important
- Partitioning
- Optimizing with partitioners
- Wide vs. Narrow dependencies

#### Week 4: SQL, Dataframes and Datasets
- Structured vs. unstructured data
- Spark SQL
- Dataframes
- Datasets
- Assignment: Data analysis on American Time Use Survey data
