"""
# We need to install numpy as it is a requirement for the ALS algorithm.  Use below commands to install numpy and download the input data.   

wget http://sourceforge.net/projects/numpy/files/NumPy/1.10.2/numpy-1.10.2.tar.gz/download --no-check-certificate
tar xzvf numpy-1.10.2.tar.gz
cd numpy-1.10.2

sudo python setup.py install

Let’s download a public movielens data with 1 million ratings from 6040 users on 3706 movies from http://files.grouplens.org/datasets/movielens/ml-1m.zip .   Once downloaded copy the files to HDFS.  

wget http://files.grouplens.org/datasets/movielens/ml-1m.zip
unzip ml-1m.zip
cd ml-1m
hadoop fs -put movies.dat movies.dat
hadoop fs -put ratings.dat ratings.dat

movies.dat has a format of movie id, movie name and movie genre as shown below. 

1::Toy Story (1995)::Animation|Children's|Comedy
2::Jumanji (1995)::Adventure|Children's|Fantasy
3::Grumpier Old Men (1995)::Comedy|Romance
4::Waiting to Exhale (1995)::Comedy|Drama
5::Father of the Bride Part II (1995)::Comedy 

ratings.dat has a format of user id, movie id, rating and timestamp as shown below. 

1::1193::5::978300760
1::661::3::978302109
1::914::3::978301968
1::3408::4::978300275
1::2355::5::978824291

Now, let’s get into the PySpark shell to interactively work with these datasets and build a recommendation system.  Alternatively, you can use IPython notebook or Zeppelin notebook for executing below commands.  For execution in Yarn mode, change the master to yarn-client. 

pyspark --master local[*] 

Once you are in the shell, import ALS algorithm related dependencies and create functions to parse the movies and ratings datasets.  
"""


from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

def parseMovie(line):
     fields = line.strip().split("::")
     return int(fields[0]), fields[1]
 
def parseRating(line):
     fields = line.strip().split("::")
     return int(fields[0]), int(fields[1]), float(fields[2])

# Create RDDs for movies and ratings by parsing them using the above parse functions. Calculate the number of ratings, users and movies.   

moviesRDD = sc.textFile("movies.dat").map(parseMovie)
ratingsRDD = sc.textFile("ratings.dat").map(parseRating)

numRatings = ratingsRDD.count()
numUsers = ratingsRDD.map(lambda r:r[0]).distinct().count()
numMovies = ratingsRDD.map(lambda r: r[1]).distinct().count()

print "Ratings dataset has %d ratings from %d users on %d movies." % (numRatings, numUsers, numMovies)

# Ratings dataset has 1000209 ratings from 6040 users on 3706 movies.

#  Explore data with DataFrames. Now, let’s create DataFrames for movie and ratings RDDs to interactively explore datasets using SQL.

movieSchema = ['movieid', 'name']
ratingsSchema = ['userid', 'movieid', 'rating']
moviesDF  = moviesRDD.toDF(movieSchema)
ratingsDF = ratingsRDD.toDF(ratingsSchema)

moviesDF.printSchema()
ratingsDF.printSchema()

# Let’s register the Dataframes as temporary tables. 
ratingsDF.createTempView("ratings")
moviesDF.createTempView("movies")

# Now, get the max, min ratings along with the count of users who have rated a movie.

ratingStats = spark.sql(
   """select movies.name, movieratings.maxrtng, movieratings.minrtng, movieratings.cntusr
     from(SELECT ratings.movieid, max(ratings.rating) as maxrtng,
     min(ratings.rating) as minrtng, count(distinct(ratings.userid)) as cntusr 
     FROM ratings group by ratings.movieid ) movieratings
     join movies on movieratings.movieid=movies.movieId
     order by movieratings.cntusr desc""")

ratingStats.show(5)

# Show the top 10 most-active users and how many times they rated a movie. 

mostActive = spark.sql(
   """SELECT ratings.userid, count(*) as cnt from ratings
             group by ratings.userid order by cnt desc limit 10""")
  
mostActive.show(5)

# Userid 4169 is the most active user.   Let’s find the movies that user 4169 rated higher than 4. 

user4169 = spark.sql("""SELECT ratings.userid, ratings.movieid,
   ratings.rating, movies.name FROM ratings JOIN movies
   ON movies.movieId=ratings.movieid
   where ratings.userid=4169 and ratings.rating > 4""")
 
user4169.show(5)

# Use randomSplit method to create training data and test data and cache them so that iterative ALS algorithm will perform better.  

RDD1, RDD2 = ratingsRDD.randomSplit([0.8, 0.2])
trainingRDD = RDD1.cache()
testRDD = RDD2.cache()
trainingRDD.count()
# 800597                                                                          
testRDD.count()
# 199612                                                                          

# Create Model

rank = 10
numIterations = 10
model = ALS.train(trainingRDD, rank, numIterations)

# Now, Get the top 5 movie predictions for user 4169 from the model generated. 
 
user4169Recs = model.recommendProducts(4169, 5)                               
user4169Recs

# [Rating(user=4169, product=128, rating=5.6649367937005231), Rating(user=4169, product=2562, rating=5.526190642914254), Rating(user=4169, product=2503, rating=5.2328684996745327), Rating(user=4169, product=3245, rating=5.1980663524880235), Rating(user=4169, product=3950, rating=5.0785092078435197)]

# Let’s evaluate the model by comparing predictions generated with real ratings in testRDD.  FirstLet’s remove the ratings from testRDD to create user ID and movie ID pairs only. Once this movie id and user id pairs are passed to the model, it will generate the predicted ratings.  

testUserMovieRDD = testRDD.map(lambda x: (x[0], x[1]))

testUserMovieRDD.take(2)
[(1, 661), (1, 3408)]

predictionsTestRDD = model.predictAll(testUserMovieRDD).map(lambda r: ((r[0], r[1]), r[2]))

predictionsTestRDD.take(2)
[((4904, 1320), 4.3029711294149289), ((4904, 3700), 4.3938405710892967)]

# Let’s transform the testRDD in key, value ((user id, movie id), rating)) format and then join with the predicted ratings as shown below.  

ratingsPredictions = testRDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictionsTestRDD)

ratingsPredictions.take(5)

#[((5636, 380), (3.0, 2.5810444309550147)), ((5128, 480), (4.0, 3.8897996775001684)), ((5198, 248), (1.0, 1.9741132086395059)), ((2016, 3654), (4.0, 4.2239704909063338)), ((4048, 256), (4.0, 4.1190428484234198))]

# Above prediction result indicates that for user ID 5636 and movie ID 380, actual rating from test data was 3.0 and predicted rating is 2.5.  Similarly, for user ID 5128 and movie ID 480, actual rating was 4.0 and predicated rating is 3.8. 
# Check accuracy of model. Now, let’s check the model to see how many bad predictions are generated by finding predicted ratings which were >= 4 when the actual test rating was <= 1.  

badPredictions = ratingsPredictions.filter(lambda r: (r[1][0] <= 1 and r[1][1]) >= 4)
badPredictions.take(2)
#[((2748, 1080), (1.0, 4.0622434036284556)), ((4565, 175), (1.0, 4.728689683016448))]
badPredictions.count()
#395   

# So, there are 395 bad predictions out of 199,612 test ratings. Next let’s evaluate the model using Mean Squared Error (MSE).  MSE is the differences between the predicted and actual targets.

MeanSquaredError = ratingsPredictions.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MeanSquaredError))                      

# Mean Squared Error = 0.797355258111. 
# Lower the MSE number, better the predictions are.  
