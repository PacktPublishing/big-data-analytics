#  Run below code in SparkR Shell

# Create a local DataFrame, convert it to Spark DataFrame.

DF <- suppressWarnings(createDataFrame(iris))
trainingDF <- DF
testDF <- DF

# Create the model to cluster them based on Sepal and Petal lengths and widths. 

model <- spark.kmeans(trainingDF, ~ Sepal_Length + Sepal_Width + Petal_Length + Petal_Width,  k = 3)

summary(model)

showDF(fitted(model))

# Now, create the clusters using the test dataset.

clusters <- predict(model, testDF)

# Now,let’s print the data from 3 different clusters.

showDF(clusters, numRows=150)

head(count(groupBy(clusters, "Species","Prediction")))
