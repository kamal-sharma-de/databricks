#As of now it is just reads the data from open dataset
from pyspark import SparkFiles

data_url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
sc.addFile(data_url)
 
path  = SparkFiles.get('rows.csv')
#print(path)
df = spark.read.csv("file://" + path, header=True, inferSchema= True, sep = ",")
df.display()
