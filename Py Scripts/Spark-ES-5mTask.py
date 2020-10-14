#!/usr/bin/env python
# ### Finding locally installed Spark and importing PySpark libraries
import findspark
from elasticsearch import Elasticsearch
import requests
from pprint import pprint
import time

findspark.init("/usr/local/spark/")
from pyspark.sql import SparkSession, functions as func


class sparkESTask(object):
    def connection(self):
        try:
            res = requests.get('http://localhost:9200')
            pprint(res.content)
        except:
            print("Connection failed! Check if Elasticsearch process has been started.")
            exit(1)

    def sparkDFtoES(self, file, name):
        try:
            # ### Reading 5 million data CSV with Spark by loading the file
            self.df = spark.read.format("csv").option("header", "true").load(filepath).fillna(0)[
                "Emp ID", "First Name", "Last Name", "Gender", "E Mail", "Month Name of Joining", "SSN", "County", "State", "Region", "City", "Zip", "Salary", func.regexp_replace(
                    func.col("Last % Hike"), "%", "").alias("Salary Hike")]
            self.df.show(5)

            # ### Checking the total records in the PySpark Dataframe
            pprint(f"Total records present: {self.df.count()}")

            # ### Checking the data types
            pprint(f"Checking the data types:\n{self.df.dtypes}")

            # ### Casting 'string' value to 'int'
            print("Casting 'string' value to 'int'")
            self.df = self.df.withColumn("Emp ID", self.df["Emp ID"].cast("int"))
            self.df = self.df.withColumn("Zip", self.df["Zip"].cast("int"))
            self.df = self.df.withColumn("Salary", self.df["Salary"].cast("int"))
            self.df = (self.df.withColumn("Salary Hike", self.df["Salary Hike"].cast("int")))
            print(self.df.dtypes)

            # ### Checking if the changes have been made successfully
            print("Checking if the changes have been made successfully")
            self.df.show()

            # ### Checking the Schema
            print("Checking the Schema")
            self.df.printSchema()

            print("Sending data to Elasticsearch by PySpark...")
            time.sleep(1)

            self.df.write.format(
                "org.elasticsearch.spark.sql"
            ).option(
                "es.resource", '%s' % (name)
            ).option(
                "es.nodes", 'localhost'
            ).option(
                "es.port", '9200'
            ).save()

            print("Successfully ingested data into Elasticsearch!")
            time.sleep(1)

            print("\nTotal number of records:")
            count = es.count(index=name)
            pprint(count)
        except:
            print("Something went wrong in sparkDF()!")

    def task1(self, name):
        try:
            # ### Task 1: Count the number of employees in each County, Region and City
            print("No. of Employees Per County:")
            county = es.search(index="humans", size=0, body={"aggs": {"No. of Employees per County": {
                "terms": {"field": "County.keyword", "size": 5, "order": {"_key": "asc"}}}}})
            pprint(county["aggregations"]["No. of Employees per County"]["buckets"])

            print("No. of Employees Per Region:")
            region = es.search(index="humans", size=0, body={"aggs": {
                "No. of Employees per Region": {"terms": {"field": "Region.keyword", "order": {"_key": "asc"}}}}})
            pprint(region["aggregations"]["No. of Employees per Region"]["buckets"])

            print("No. of Employees Per City:")
            city = es.search(index="humans", size=0, body={"aggs": {"No. of Employees per City": {
                "terms": {"field": "City.keyword", "size": 5, "order": {"_key": "asc"}}}}})
            pprint(city["aggregations"]["No. of Employees per City"]["buckets"])
        except:
            print("Something went wrong in Task1!")

    def task2(self, name):
        try:
            # ### Task 2: Generate employee summary
            print("Employee Summary:")
            empSummary = es.search(index="humans", size=5, body={"query": {"match_all": {}}})
            pprint(empSummary["hits"]["hits"])
        except:
            print("Something went wrong in Task2!")

    def task3(self, name):
        try:
            # ### Task 3: Generate employee summary and ordering by Gender and Salary
            print("Employee summary and ordering by Gender and Salary:")
            empSummaryGS = es.search(index="humans", size=5, body={
                "sort": [{"Gender.keyword": {"order": "asc"}}, {"Salary": {"order": "asc"}}]})
            pprint(empSummaryGS["hits"]["hits"])
        except:
            print("Something went wrong in Task3!")

    def task4(self, name):
        try:
            # ### Task 4: Summerize the number of employee joined and hikes granted based on month
            print("Number of employee joined based on month:")
            empMonth = es.search(index="humans", size=0, body={"aggs": {"No. of Employees joined in particular Month": {
                "terms": {"field": "Month Name of Joining.keyword", "size": 12}}}})
            pprint(empMonth["aggregations"]["No. of Employees joined in particular Month"]["buckets"])

            print("Number of employee joined based on hikes granted:")
            empHike = es.search(index="humans", size=0, body={"aggs": {
                "Month": {"terms": {"field": "Month Name of Joining.keyword", "size": 12},
                          "aggs": {"Hikes granted in this Month": {"terms": {"field": "Salary Hike"}}}}}})
            pprint(empHike["aggregations"]["Month"]["buckets"])
        except:
            print("Something went wrong in Task4!")

    def task5(self, name):
        try:
            # ### Task 5: Generate employee summary and ordering by Salary
            print("Employee summary and ordering by Salary:")
            empSalary = es.search(index="humans", size=5,
                                  body={"query": {"match_all": {}}, "sort": [{"Salary": {"order": "asc"}}]})
            pprint(empSalary["hits"]["hits"])
        except:
            print("Something went wrong in Task5!")

    def deleteIndex(self, name):
        try:
            es.indices.delete(index=name)
            print(f"{indexName} index deleted successfully!")
        except:
            print(f"Error deleting {name} Index")

    def run(self):
        running = True
        try:
            self.sparkDFtoES(filepath, indexName)
            while running:
                print("-----------------------------------------")
                print("1.Count the number of employees in each County, Region and City\n"
                      "2.Generate employee summary\n"
                      "3.Generate employee summary and ordering by Gender and Salary\n"
                      "4.Summerize the number of employee joined and hikes granted based on month\n"
                      "5.Generate employee summary and ordering by Salary\n"
                      "6.Delete Index?\n"
                      "E.TO EXIT")
                print("-----------------------------------------")
                mode = input("Enter your choice: ")

                if mode == "1":
                    self.task1(indexName)
                elif mode == "2":
                    self.task2(indexName)
                elif mode == "3":
                    self.task3(indexName)
                elif mode == "4":
                    self.task4(indexName)
                elif mode == "5":
                    self.task5(indexName)
                elif mode == "6":
                    self.deleteIndex(indexName)
                elif mode.upper() == "E":
                    break
                else:
                    print("Invalid input! Try Again!")
        except:
            print("Something went wrong in run()!")


if __name__ == "__main__":
    filepath = "/home/ubuntu/Hr5m.csv"
    start = sparkESTask()
    start.connection()
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    # ### Entering index name
    indexName = input("Enter the elasticsearch index name: ")
    # ### Creating Spark Session
    spark = SparkSession.builder.appName('task').getOrCreate()
    print("SparkSession Started Successfully!")
    start.run()
    # ### Stopping the SparkSession
    es.transport.close()
    print("Closed connection to Elasticsearch!")
    spark.stop()
    print("SparkSession Stopped Successfully!")
