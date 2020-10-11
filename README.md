# Processing huge data with Spark and Elasticsearch
This task had the following questions to solve on huge data:

**Task 1:** Count the number of employees in each County, Region and City<br/>
**Task 2:** Generate Employee Summary<br/>
**Task 3:** Generate employee summary and ordering by Gender and Salary<br/>
**Task 4:** Summerize the number of employee joined and hikes granted based on month<br/>
**Task 5:** Generate employee summary and ordering by Salary

This task has been performed on 5 million records CSV data using PySpark and Elastisearch (No Logstash!). I performed this task in 2 ways:
1. Using only **PySpark**
2. Using **PySpark and Elasticsearch**.
#
## Prerequisites:
- Python3 (**less than Python3.8** to avoid compatibility issues)
```
$ sudo apt-get install python3
$ sudo apt-get install python3-pip
```
- **[Java JDK8](https://jdk.java.net)** (required for **Spark**) and **[JDK11](https://jdk.java.net)** (required for **Elasticsearch**)
- **[Apache Spark](https://spark.apache.org/downloads.html)** (**v2.4.x** preferable to avoid compatibility issues) and also install the **PySpark** using pip
```
$ sudo pip3 install pyspark
```
- **[Elasticsearch](https://www.elastic.co/downloads/elasticsearch)** and also install the **Elasticsearch** using pip
```
$ sudo pip3 install elasticsearch
```
- **[ES Hadoop jar](https://www.elastic.co/downloads/hadoop)**
- **[5million records dataset](http://eforexcel.com/wp/downloads-16-sample-csv-files-data-sets-for-testing/)**
#
## Steps to run the code
### Basic Steps:
As this task has been performed by two types, the first few steps are similar and then they branch out differently. So in the basics steps, the process of loading the data with PySpark into a dataframe has to be done. The dataset used for this task was already clean, so no data cleaning procedures had to be performed. But in some cases, this might be a necessicity as in most cases the data is noisy and dirty, so perform the steps as required by your dataset.

1. First import the necessary libraries required for the task.
```python3
findspark.init("/usr/local/spark/") # finding locally installed spark
from pyspark.sql import SparkSession, functions as func
```
2. Next create a **SparkSession**.
```
spark = SparkSession.builder.appName('task').getOrCreate()
```
3. Keep in mind since the dataset is huge, reading the data with spark would sometimes cause the kernel to die as in case of Jupyter Notebooks, so we need to load the data with spark and make a dataframe.
```
df = spark.read.format("csv").option("header","true").load("/your/dataset/path").fillna(0)["the columns you need"]
```
4. Next check the data types of each of the chosen columns. Since there might be some columns that contains numeric data but are of string data type, we need to type cast them into appropriate data type.
```
df.dtypes # for checking the data types
df = df.withColumn("your column", df["your column"].cast("your data type"))
```
5. Make sure if all the necessary changes have been done by checking all the data types and the data itself.
```
df.types
df.show()
```
#
### PySpark Task:
After the **[basic steps](https://github.com/Wolvarun9295/Spark-Elasticsearch-5MilData#basic-steps)** have been performed, the task using PySpark is fairly easy to do since we have to apply groupby and aggregation functions using PySpark. After completion of the task **make sure to stop the SparkSession**.
```
spark.stop()
```
#
### PySpark + Elasticsearch Task:
So for doing this task using PySpark and Elasticsearch, first we'll extract the Elasticsearch tar file and need to add a few extra libraries to the basic steps we just performed as follows
```
from elasticsearch import Elasticsearch
import requests
from pprint import pprint
```
1. First we will have to start the Elasticsearch from the terminal.
```
$ cd /path/to/elasticsearch
$ ./bin/elasticsearch # to see all the details

---OR---

$ ./bin/elasticsearh -d # to start elasticsearch as deamon process
```
2. Next extract the **ES Hadoop zip** folder and copy the **elasticsearch-hadoop-x.jar** to the spark **jars** folder.
```
$ cd /es-hadoop/dist
$ cp elasticsearch-hadoop-x.jar /path/to/spark/jars
```
3. Next check if Elasticsearch is successfully reachable or not by creating a requests object.
```
res = requests.get('http://localhost:9200')
pprint(res.content)
```
4. After the **[basic steps](https://github.com/Wolvarun9295/Spark-Elasticsearch-5MilData#basic-steps)** have been performed successfully, we need to write the PySpark dataframe to the Elasticsearch directly in much less time without the need of Logstash since Logstash is time and memory hog.
```
df.write.format(
    "org.elasticsearch.spark.sql"
).option(
    "es.resource", '%s' % ('your indexname')
).option(
    "es.nodes", 'localhost'
).option(
    "es.port", '9200'
).save()
```
5. Next create an Elasticsearch object and perform the tasks as required.
```
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
```
6. During the tasks with elasticsearch, when performing aggregation operations on strings, we need to use "**.keyword**" after the string columns that need to be used since string columns aren't allowed to be iterated by default.
7. And after the tasks have been successfully performed, **close the connection to Elasticsearch and stop the SparkSession**.
```
es.transport.close()
spark.stop()
```
#
### Using Jupyter Notebook or Python Script:
This task has been done in Jupyter Notebook as well as in Python Script.

The code in Jupyter Notebook is straightforward and can be run sequentially and can be seen rendered on GitHub.

The code in Python Script is similar but uses class and methods for respective tasks and runs as follows:
<img src="https://github.com/Wolvarun9295/Spark-Elasticsearch-5MilData/blob/master/Screenshots/pyfile.png"></img>
#
## References:
- [A Basic Guide To Elasticsearch Aggregations](https://logz.io/blog/elasticsearch-aggregations/)
- [Indexing into Elasticsearch using Spark - code snippets](https://medium.com/@akkidx/indexing-into-elasticsearch-using-spark-code-snippets-55eabc753272)
- [Python Elasticsearch Client API Documentation](https://elasticsearch-py.readthedocs.io/en/master/index.html)
- [23 Useful Elasticsearch Example Queries](https://dzone.com/articles/23-useful-elasticsearch-example-queries)
- [Elasticsearch Tutorial for beginners - TechieLifestyle](https://www.youtube.com/playlist?list=PLGZAAioH7ZlO7AstL9PZrqalK0fZutEXF)