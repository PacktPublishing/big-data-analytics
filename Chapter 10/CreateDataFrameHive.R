# Get into SparkR shell and run these commands.

results <- sql("SELECT * from sample_07 limit 10")
results

# DataFrame[code:string, description:string, total_emp:int, salary:int]

head(results)

avg <- sql("select avg(salary) as avg_salary from sample_07")
collect(avg)


