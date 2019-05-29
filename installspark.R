# Pre-installation script for Intermediate R Programming / Spark and R with Sparklyr

# General -----------------------------------------------------------------

install.packages(c("mangoTraining", "tidyverse"))

# Intermediate R ----------------------------------------------------------

install.packages(c("profvis", "microbenchmark"))

# Sparklyr ----------------------------------------------------------------

install.packages("sparklyr")

library(sparklyr)
library(dplyr)

# The sparklyr package contains a function that installs Spark:
# spark_install()
# Spark 2.4.3 for Hadoop 2.7 or later already installed.

# After installing spark you may need to restart your R session. 
# After restarting try to execute the following code to verify Spark is working.


sc <- spark_connect(master = "local")
# Check the console for errors.

copy_to(sc, iris)
# Check the console for errors.

spark_disconnect_all()

# Please also download all of the data files from GitHub:
# https://github.com/MangoTheCat/spark-and-r-with-sparklyr-course
