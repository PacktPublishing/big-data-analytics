#  Run below code in SparkR Shell

# Create a local DataFrame, convert it to Spark DataFrame.

localDF <- as.data.frame(Titanic)
DF <- createDataFrame(localDF[localDF$Freq > 0, -5])
head(DF)
count(DF)

#Create training and test datasets from the DataFrame.

trainingDF <- DF
testDF <- DF

# Create the model from training dataset and then print.

model <- spark.naiveBayes(trainingDF, Survived ~ Class + Sex + Age)

summary(model)

# Now, predict survivals using the test dataset.

survivalPredictions <- predict(model, testDF)

survivalPredictions 

showDF(select(survivalPredictions, "Class", "Survived", "prediction"))
