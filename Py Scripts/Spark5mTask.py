#!/usr/bin/env python
# ### Finding locally installed Spark and importing PySpark libraries
from itertools import count

import findspark

findspark.init("/usr/local/spark/")
from pyspark.sql import SparkSession, functions as func


class sparkTask(object):
    def sparkDF(self, file):
        try:
            # ### Reading 5 million data CSV with Spark by loading the file
            self.df = spark.read.format("csv").option("header", "true").load(filepath).fillna(0)[
                "Emp ID", "First Name", "Last Name", "Gender", "E Mail", "Month Name of Joining", "SSN", "County", "State", "Region", "City", "Zip", "Salary", func.regexp_replace(
                    func.col("Last % Hike"), "%", "").alias("Salary Hike in %")]
            self.df.show(5)

            # ### Checking the total records in the PySpark Dataframe
            print(f"Total records present: {self.df.count()}")

            # ### Checking the data types
            print(self.df.dtypes)

            # ### Casting 'string' value to 'int'
            self.df = self.df.withColumn("Emp ID", self.df["Emp ID"].cast("int"))
            self.df = self.df.withColumn("Zip", self.df["Zip"].cast("int"))
            self.df = self.df.withColumn("Salary", self.df["Salary"].cast("int"))
            self.df = (self.df.withColumn("Salary Hike in %", self.df["Salary Hike in %"].cast("int")))
            print(self.df.dtypes)

            # ### Checking if the changes have been made successfully
            self.df.show(5)

            # ### Checking the Schema
            self.df.printSchema()

            # return self.df
        except:
            print("Something went wrong in sparkDF()!")

    def task1(self):
        df = self.df
        try:
            # ### Task 1: Count the number of employees in each County, Region and City
            print("No. of Employees Per County:")
            countyCount = df.agg(func.countDistinct("County").alias("Counties")).collect()
            countyCount = countyCount[0].Counties
            task11 = df.groupBy("County").count().sort("County")
            task11.show(countyCount)

            print("No. of Employees Per Region:")
            regionCount = df.agg(func.countDistinct("Region").alias("Regions")).collect()
            regionCount = regionCount[0].Regions
            task12 = df.groupBy("Region").count().sort("Region")
            task12.show(regionCount)

            print("No. of Employees Per City:")
            cityCount = df.agg(func.countDistinct("City").alias("Cities")).collect()
            cityCount = cityCount[0].Cities
            task13 = df.groupBy("City").count().sort("City")
            task13.show(cityCount)
        except:
            print("Something went wrong in Task1!")

    def task2(self):
        df = self.df
        try:
            # ### Task 2: Generate employee summary
            print("Employee Summary:")
            task2 = df.sort("Emp ID")
            task2.show(5)
        except:
            print("Something went wrong in Task2!")

    def task3(self):
        df = self.df
        try:
            # ### Task 3: Generate employee summary and ordering by Gender and Salary
            print("Employee summary and ordering by Gender and Salary:")
            task3 = df.orderBy(["Gender", "Salary"])
            task3.show(5)
        except:
            print("Something went wrong in Task3!")

    def task4(self):
        df = self.df
        try:
            # ### Task 4: Summerize the number of employee joined and hikes granted based on month
            print("Number of employee joined based on month:")
            task41 = df.groupBy("Month Name of Joining").count()
            task41.show()

            print("Number of employee joined based on hikes granted:")
            hikesCount = df.agg(func.countDistinct("Month Name of Joining","Salary Hike in %").alias("Hikes_Per_Month")).collect()
            hikesCount = hikesCount[0].Hikes_Per_Month
            task42 = df.groupBy("Month Name of Joining", "Salary Hike in %").count().sort("Month Name of Joining")
            task42.show(hikesCount)
        except:
            print("Something went wrong in Task4!")

    def task5(self):
        df = self.df
        try:
            # ### Task 5: Generate employee summary and ordering by Salary
            print("Employee summary and ordering by Salary:")
            task5 = df.orderBy(["Salary"])
            task5.show(5)
        except:
            print("Something went wrong in Task5!")

    def run(self):
        running = True
        try:
            self.sparkDF(filepath)
            while running:
                print("-----------------------------------------")
                print("1.Count the number of employees in each County, Region and City\n"
                      "2.Generate employee summary\n"
                      "3.Generate employee summary and ordering by Gender and Salary\n"
                      "4.Summerize the number of employee joined and hikes granted based on month\n"
                      "5.Generate employee summary and ordering by Salary\n"
                      "E.TO EXIT")
                print("-----------------------------------------")
                mode = input("Enter your choice: ")

                if mode == "1":
                    self.task1()
                elif mode == "2":
                    self.task2()
                elif mode == "3":
                    self.task3()
                elif mode == "4":
                    self.task4()
                elif mode == "5":
                    self.task5()
                elif mode.upper() == "E":
                    break
                else:
                    print("Invalid input! Try Again!")
        except:
            print("Something went wrong in run()!")


if __name__ == "__main__":
    filepath = "/home/ubuntu/Hr5m.csv"
    # ### Creating Spark Session
    spark = SparkSession.builder.appName('task').getOrCreate()
    print("SparkSession Started Successfully!")
    start = sparkTask()
    start.run()
    # ### Stopping the SparkSession
    spark.stop()
    print("SparkSession Stopped Successfully!")
