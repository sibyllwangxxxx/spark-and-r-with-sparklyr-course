library(sparklyr)
library(dplyr)


flights <- spark_read_parquet(sc, 
                              name = "flights_sc",
                              path = "Data/2008.parquet/")
colnames(flights)
dim(flights)
tbl_vars(flights)
dim(flights_sc)





## plane is the R object that you interact with in R 
## which translates your R code to operate on the actual tabular data saved on the Spark cluster 
plane <- spark_read_csv(sc,
                        name = "plane_sc",
                        path = "Data/plane-data.csv")
colnames(plane)
dim(plane)
tbl_vars(plane)






airports <- spark_read_csv(sc,
                           name = "airports_sc",
                           path = "Data/airports.csv")
colnames(airports)
dim(airports)
tbl_vars(airports)
print(airports)







carriers <- spark_read_csv(sc,
                           name = "carriers_sc",
                           path = "Data/carriers.csv")
colnames(carriers)
dim(carriers)
tbl_vars(carriers)
print(carriers)





## working with dplyr on spark ------------------------------------------------------------
## have a peek of the first few rows without really executing the queries
friday <- flights %>%
          filter(DayOfWeek == 5) %>%
          select(-DayOfWeek) %>%
          mutate(Date = paste(Year, Month, DayofMonth, sep = "-"))

friday %>% select(Year, Month, DayofMonth, Date) %>% head(5)


## actually execute the code stored in friday
collect(friday)


## show SQL queries
show_query(friday)








## exercise Page 2-6
fifteen <- flights %>%
           filter(DepDelay > 15 & DepDelay < 240, DayofMonth == 15) %>%
           select(Year, Month, ArrDelay, DepDelay, Distance, UniqueCarrier) 
show_query(fifteen)

collect(fifteen)







## sampling
flights %>% sample_n(5) %>% collect()
flights %>% sample_n(15) %>% collect()

flights5_1 <- flights %>%
              sample_n(5) %>%
              collect()

flights5_2 <- flights %>%
              sample_n(5) %>%
              collect()

all.equal(flights5_2, flights5_1)




## creating new spark data frames
sdf_register(friday, "fridayflights")
tbl_cache(sc, "fridayflights")

## bring it back to R
fridayflights <- tbl(sc, "fridayflights")
all.equal(friday, fridayflights)
all.equal(collect(friday), collect(fridayflights))





## join, register and cache
monday <- flights %>%
          filter(Origin == "SEA" & DayOfWeek == 1) %>%
          left_join(airports, by = c(Dest = "iata"))

sdf_register(monday, "monday")
tbl_cache(sc, "monday")


set.seed(1)
tmp1 <- tbl(sc, "monday") %>%
          sample_frac(0.2) %>%
          collect()
set.seed(1)
tmp2 <- tbl(sc, "monday") %>%
          sample_frac(0.2) %>%
          collect()
all.equal(tmp1, tmp2)





























