#!/usr/bin/env python
# ### Finding locally installed Spark and importing PySpark libraries
import findspark

findspark.init("/usr/local/spark/")
from pyspark.sql import SparkSession, functions as func


class sparkTask(object):
    def sparkDF(self, file):
        try:
            # ### Reading CSV with Spark
            self.df = spark.read.csv(file, header=True, inferSchema=True).fillna(0)[
                "Emp ID", "Month Name of Joining", "Last Name", "Gender", "E Mail", "SSN", "County", "State", "Region", "City", "Zip", "Salary", func.regexp_replace(
                    func.col("Last % Hike"), "%", "").alias("Salary Hike")]
            self.df.show(5)

            # ### Checking the total records in the PySpark Dataframe
            print(f"Total records present: {self.df.count()}")

            # ### Checking the data types
            print(self.df.dtypes)

            # ### Casting 'string' value to 'int'
            self.df = (self.df.withColumn("Salary Hike", self.df["Salary Hike"].cast("int")))
            print(self.df.dtypes)

            # ### Checking if the changes have been made successfully
            self.df.show()

            # ### Checking the Schema
            self.df.printSchema()

            return self.df
        except:
            print("Something went wrong in sparkDF()!")

    def task1(self):
        df = self.df
        try:
            # ### Task 1: Count the number of employees in each County, Region and City
            print("No. of Employees Per County:")
            task11 = df.groupBy("County").agg(func.countDistinct("Emp ID").alias('No. of Employees Per County'))
            task11.show(5)

            print("No. of Employees Per Region:")
            task12 = df.groupBy("Region").agg(func.countDistinct("Emp ID").alias('No. of Employees Per Region'))
            task12.show(5)

            print("No. of Employees Per City:")
            task13 = df.groupBy("City").agg(func.countDistinct("Emp ID").alias('No. of Employees Per City'))
            task13.show(5)
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
            task41 = df.groupBy("Month Name of Joining").agg(
                func.countDistinct("Emp ID").alias('No. of Employees Joined in Particular Month'))
            task41.show()

            print("Number of employee joined based on hikes granted:")
            task42 = df.groupBy("Month Name of Joining").agg(
                func.countDistinct("Salary Hike").alias('No. of Hikes granted in Particular Month'))
            task42.show()
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


filepath = "/home/ubuntu/10000 Records.csv"
# ### Creating Spark Session
spark = SparkSession.builder.appName('task').getOrCreate()
print("SparkSession Started Successfully!")
start = sparkTask()
start.run()
# ### Stopping the SparkSession
spark.stop()
print("SparkSession Stopped Successfully!")
