# CONFIG ----------------------------------------------------------------------
#
# This is a configuration file that sets options for a Spark session through
# the package {sparklyr}, which is used in the "05_transition_analysis.R".
# The optimal options for Spark can vary with each computer

# Spark memory options --------------------------------------------------------
config <- spark_config()
# Create configuration object

# Use custom spark configuration?
custom_config <- TRUE
# Set to TRUE if you want to set custom configuration

if (custom_config) {

  # Storage memory
  config$`sparklyr.shell.driver-memory` <- "24G"
  # ("refers to that used for caching and propagating
  # internal data across the cluster")

  # Executor memory
  config$`sparklyr.shell.executor-memory` <- "4G"
  # ("refers to that used for computation in shuffles,
  # joins, sorts and aggregations")

  # Overhead
  config$`spark.yarn.executor.memoryOverhead` <- "1G"

  # More information:
  # https://spark.apache.org/docs/latest/tuning.html#memory-management-overview
  # https://spark.rstudio.com/guides/connections/

}
