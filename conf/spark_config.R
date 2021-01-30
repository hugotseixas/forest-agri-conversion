# CONFIG ----------------------------------------------------------------------
#
# This is a configuration file that sets options for a Spark session through
# the package {sparklyr}, which is used in the "05_transition_analysis.R".
# The optimal options for Spark can vary with each computer

# Spark memory options --------------------------------------------------------
config <- spark_config()
# Create configuration object
config$`sparklyr.shell.driver-memory` <- "16g"
# Storage memory ("refers to that used for caching and propagating
# internal data across the cluster")
config$`sparklyr.shell.executor-memory` <- "8g"
# Executor memory ("refers to that used for computation in shuffles,
# joins, sorts and aggregations")
config$`spark.yarn.executor.memoryOverhead` <- "1g"
# More information:
#   https://spark.apache.org/docs/latest/tuning.html#memory-management-overview
#   https://spark.rstudio.com/guides/connections/
