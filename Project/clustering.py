
import json
from pyspark.mllib.clustering import KMeans
#from pyspark.mllib.evaluation import ClusteringEvaluator
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType
from pyspark.mllib.linalg import Vectors
sc = SparkContext('local')
spark = SparkSession(sc)


f = open('final_player_profile.json' , 'r')
data = json.load(f)

# Loads data.
dataset = spark.read.option('multiline','true').json("final.json")
dataset.printSchema()
dataset.show()
#val df = spark.read.schema(schema).json(file).cache()
# Trains a k-means model.

#datardd = dataset.select('goals','foul','owngoal','passacc','targetshots').rdd
#datardd.cache()

dataset = dataset.filter(dataset['no_matches'] <5)
dataset.show()

dataset1 = dataset.select('goals','foul','owngoal','passacc','targetshots')
rdd = dataset1.rdd.map(lambda data: Vectors.dense([c for c in data]))
model = KMeans.train(rdd, 5, maxIterations=10, initializationMode="k-means||")
#print(model.clusterCenters)
final = model.predict(rdd)
#print(final.collect())
#print(final.countByValue().items())

list1 = dataset.select('playerId').rdd.map(lambda x: x[0]).collect()
#print(list1)

newdf= spark.createDataFrame(data=zip(final.collect(),list1) , schema= ['value','playerId'])
#newdf.show()

newdataset = dataset.join(newdf,dataset.playerId == newdf.playerId, how ='left').drop(newdf.playerId)

final = newdataset.groupBy('value').avg('rating')

#print(final[final['value'] == 0].rdd.map(lambda x: x['avg(rating)']).collect()[0])
#list2 = newdataset.select(['playerId','value']).rdd.map(lambda x,y: (x,final[final['value'] == 0].rdd.map(lambda x: x['avg(rating)']).collect()[0])).collect()
#print(list2)



#print(data)
for val in [0,1,2,3,4]:
	listofplayers= newdataset.filter(newdataset['value'] ==val).rdd.map(lambda x: x['playerId']).collect()
	avgrating = final[final['value']==val].rdd.map(lambda x: x['avg(rating)']).collect()[0]
	for i in listofplayers:
		data[str(i)]['rating'] = avgrating

f.close()
"""
newlist= data.keys()
for player in newlist:
	req = newdataset[newdataset['playerId']==int(player)].rdd.map(lambda x: x['value']).collect()[0]
	rating = final[final['value'] == req].rdd.map(lambda x: x['avg(rating)']).collect()[0]
	print(req,rating)
	data[player]['rating'] = rating
f.close()
"""



f = open('final_player_profile.json' , 'w')
f.write(json.dumps(data))
f.close()

#final.show()
#newdataset.show()
#print(newdataset.count())
#print(dataset.count() , newdf.count())



