# Get into SparkR shell and run these commands.

localDF <- data.frame(name=c("Jacob", "Jessica", "Andrew"), age=c(48, 45, 25))

df1 <- as.DataFrame(localDF)
df2 <- createDataFrame(localDF)

collect(df1)
collect(df2)


