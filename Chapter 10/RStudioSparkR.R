# Run this script in R Studio

.libPaths(c(.libPaths(), '/home/cloudera/spark-2.0.0-bin-hadoop2.7/R/lib'))

Sys.setenv(SPARK_HOME = '/home/cloudera/spark-2.0.0-bin-hadoop2.7')

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--master" "yarn" "--packages" "com.databricks:spark-avro_2.11:3.0.0" "sparkr-shell"')

Sys.setenv(PATH = paste(Sys.getenv(c('PATH')), '/home/cloudera/spark-2.0.0-bin-hadoop2.7/bin', sep=':'))

library(SparkR)
sparkR.session(appName = "RStudio Application")

library(magrittr)

flights <- read.df("flights.csv", source = "csv", header = "true")

# Run a query to print the top most frequent destinations from JFK airport

jfk_dest <- filter(flights, flights$origin == "JFK") %>% 
  group_by(flights$dest) %>% 
  summarize(count = n(flights$dest))

top_dests <- head(arrange(jfk_dest, desc(jfk_dest$count)))

# Finally, create a bar plot of top destinations.   
barplot(top_dests$count, names.arg = top_dests$dest,col=rainbow(7),main="Top Flight Destinations from JFK", xlab = "Destination", ylab= "Count", beside=TRUE )
