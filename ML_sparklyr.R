library(sparklyr)
library(dplyr)

## ML with sparklyr -----------------------------------
sc <- spark_connect(master = "local", app_name = "mango")
iris_tbl <- copy_to(sc, iris)

iris_tbl %>%
  ft_binarizer(input_col = "Sepal_Length",
               output_col = "SL_Threshold",
               threshold = 5) %>%
  select(Sepal_Length, SL_Threshold)


## exercise Page 3-4
monday <- flights %>%
          filter(Origin == "SEA" & DayOfWeek == 1) %>%
          left_join(airports, by = c(Dest = "iata")) %>%
          mutate(DepTime_num = as.numeric(DepTime)) %>%
          mutate(DepDelay_num = as.numeric(DepDelay)) %>%
          ft_binarizer(input_col = "DepTime_num",
                       output_col = "morning",
                       threshold = 1200) %>%
          ft_bucketizer(input_col = "DepDelay_num", 
                        output_col = "late",
                        #splits = c(0, 15, 60)) did not work
                        splits = c(-Inf, 0, 15, 60, Inf))

monday %>% select(DepTime_num, morning, late)





##  model training
iris_tbl <- iris_tbl %>%
              select(Sepal_Length, Sepal_Width) %>%
              na.omit()

iris_split <- sdf_random_split(iris_tbl, training = 0.8, testing = 0.2)

model <- iris_split$training %>%
          ml_linear_regression(formula = Sepal_Length ~ Sepal_Width)
model
attributes(model)
summary(model)
tidy(model)


## exercise Page 3-7
monday <- monday %>%
            select(Cancelled, CRSDepTime, Dest) %>%
            na.omit()

monday_split <- sdf_random_split(monday, training = 0.8, testing = 0.2)

monday_mod <- monday_split$training %>%
                ml_logistic_regression(formula = Cancelled ~ ., family = "binomial")

fit<-glm(data = collect(monday_split$training), formula = Cancelled ~ ., family=binomial())

tidy(monday_mod)
summary(monday_mod)
summary(fit)

scores <- monday_mod %>%
            ml_predict(monday_split$testing)

pred <- cbind(collect(scores), predict(fit, newdata = monday_split$testing))


ml_regression_evaluator(scores, 
                        label_col = "Cancelled",
                        metric_name = "rmse")


ml_binary_classification_evaluator(scores,
                                   label_col = "Cancelled",
                                   metric_name = "areaUnderROC")








## model pipelines --------------------------------------------------------

## customized pipe ≈ a function
## good for scaling, normalization, e.g. if integer change to numeric - data preprocessing etc.
iris_pipe <- . %>%
              mutate(Sepal.Width = cut(Sepal.Width, breaks = c(0, 3, 3.5, 4.5))) %>%
              lm(Sepal.Length ~ Sepal.Width + Species, data = .)

iris_pipe(iris)            
  



irisSparkPipe <- ml_pipeline(sc) %>%
                  ft_bucketizer(input_col = "Sepal_Width",
                                output_col= "Sepal_Width_Bucket",
                                splits = c(0, 3, 3.5, 4.5)) %>%
                  ft_r_formula(formula = Sepal_Width ~ Sepal_Length) %>%
                  ml_linear_regression()

irisSparkPipe$stages

iris_model <- ml_fit(irisSparkPipe, iris_split$training)
iris_model


## exercise Page 4-6
monday <- flights %>%
          filter(Origin == "SEA" & DayOfWeek == 1) %>%
          #left_join(airports, by = c(Dest = "iata")) %>%
          select(Cancelled, CRSDepTime, Dest)

# tmp <- flights %>%
#   filter(Origin == "SEA") %>%
#   filter(DayOfWeek == 1) 

sea_pipe <- ml_pipeline(sc) %>%
              # filter(Origin == "SEA" & DayOfWeek == 1) %>%
              # instead of directly inserting dplyr code in the pipeline (which does not work, line above),  
              # use ft_dplyr_transformer() to "paste" the dplyr code defined in monday into the pipeline
              #ft_dplyr_transformer(monday) %>%
              ft_r_formula(formula = Cancalled ~ Dest) %>%
              ml_logistic_regression()

sea_pipe

sea_model <- ml_fit(sea_pipe, monday_split$training)
sea_model












