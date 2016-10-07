# Let’s download the flights data and copy it to HDFS.  
# [cloudera@quickstart ~]$ wget https://s3-us-west-2.amazonaws.com/sparkr-data/nycflights13.csv --no-check-certificate
# [cloudera@quickstart ~]$ hadoop fs -put nycflights13.csv flights.csv

# To use, CSV external package, start the sparkR shell and then install and load magrittr package.  While installing packages, use http locations nearby you.  

# [cloudera@quickstart ~]$ sparkR   

install.packages("magrittr", dependencies = TRUE)
library(magrittr)

flights <- read.df("flights.csv",source="csv", header="true", inferschema="true")

flights
#DataFrame[year:int, month:int, day:int, dep_time:string, dep_delay:string, arr_time:string, arr_delay:string, carrier:string, tailnum:string, flight:int, origin:string, dest:string, air_time:string, distance:int, hour:string, minute:string]

# Cache the DataFrame in memory using below command.

cache(flights)

# Count number of rows in the flights DataFrame.  
count(flights)

# [1] 336776

# Print the first six rows from the DataFrame using head method or use showDF method to print SQL like printout.   If you want to print first 2 rows only use take method.  

head(flights)   
showDF(flights)
take(flights, 2)

# Print the statistics of each column using describe method.   

desc <- describe(flights)
collect(desc)

# Filter the columns on specific condition and display only selected columns.

filter <- filter(flights, "dep_delay > 100")
delay100 <- select(filter, c("origin", "dest", "dep_delay"))
head(delay100)

# Print the number of records group by carrier in descending order.

carriers <- count(groupBy(flights, "carrier"))
head(arrange(carriers, desc(carriers$count)))

# Run a query to print the top most frequent destinations from JFK airport.  Group the flights by destination airport from JFK and then aggregate by the number of flights and then sort by the count column.  Print the first six rows.  

jfk_origin <- filter(flights, flights$origin == "JFK")
jfk_dest <- agg(group_by(jfk_origin, jfk_origin$dest), count = n(jfk_origin$dest))

head(arrange(jfk_dest, desc(jfk_dest$count)))

# Now, register the DataFrame as a temporary table and query it using SQL. 

createOrReplaceTempView(flights, "flightsTable")
delayDF <- sql(sqlContext, "SELECT dest, arr_delay FROM flightsTable")

head(delayDF)

# Use below commands to create new column, delete columns, and rename columns.  Check the data before and after deleting the column using head method.   

flights$air_time_hr <- flights$air_time / 60
flights$air_time_hr <- NULL
newDF <- mutate(flights, air_time_sec = flights$air_time * 60)
renamecolDF <- withColumnRenamed(df, " air_time", " air_time_ren")

# Combine the whole query into two lines using magrittr

jfk_dest <- filter(flights, flights$origin == "JFK") %>% 
  group_by(flights$dest) %>% 
  summarize(count = n(flights$dest))

frqnt_dests <- head(arrange(jfk_dest, desc(jfk_dest$count)))

head(frqnt_dests)

# Finally, create a bar plot of top destinations.   

barplot(frqnt_dests$count, names.arg = frqnt_dests$dest,col=rainbow(7),main="Top Flight Destinations from JFK", xlab = "Destination", ylab= "Count", beside=TRUE )

# Though it’s not recommended, if you want to convert SparkR Dataframe to R local data frame, use below command.

localdf <- collect(jfk_dest)

