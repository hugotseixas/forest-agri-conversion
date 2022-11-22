# HEADER ----------------------------------------------------------------------
#
# Title:        Create figure 2
# Description:  The objective of this routine is to explore the dataset by
#               filtering and aggregating data in Spark. Results are supposed
#               to give us a overall view of the data by plots and tables.
#
# Author:       Hugo Tameirao Seixas
# Contact:      tameirao.hugo@gmail.com
# Date:         2020-11-10
#
# Notes:        In order to run this routine, you will need to successfully
#               install and connect to Spark using the {sparklyr} package.
#               Installation and use guide can be found in:
#               https://github.com/sparklyr/sparklyr
#
# LIBRARIES -------------------------------------------------------------------
#
library(sparklyr)
library(magrittr)
library(lubridate)
library(scales)
library(dplyr)
library(tibble)
library(readr)
library(tidyr)
library(forcats)
library(ggplot2)
library(ggtext)
library(scico)
library(gt)
#
# OPTIONS ---------------------------------------------------------------------
#
source('conf/spark_config.R')
#
# SET LULC NAMES --------------------------------------------------------------

## Get the names for each LULC class from MapBiomas ----
mb_dict <- read_csv("data/mb_class_dictionary.csv") %>%
  select(class_code, class_name) %>%
  rename(agri_code = class_code)

# LOAD DATASETS ---------------------------------------------------------------

## Connect with Spark -----
sc <- spark_connect(master = "local", config = config, version = "3.3.0")

## Load trans_length table ----
trans_length <-
  spark_read_parquet(
    sc,
    name = "trans_length",
    path = "data/trans_tabular_dataset/trans_length/",
    memory = FALSE
  )

## Load transition time series table ----
trans_classes <-
  spark_read_parquet(
    sc,
    name = "trans_classes",
    path = "data/trans_tabular_dataset/trans_classes/",
    memory = FALSE
  )

# FILTER AND AGGREGATE DATA ---------------------------------------------------

## Get LULC percentage during and after transition ----
trans_time_serie <-
  trans_length %>%
  filter(agri_cycle == 1) %>%
  inner_join(
    trans_classes %>% filter(agri_cycle == 1),
    by = c("agri_cycle", "cell_id", "tile_id")
  ) %>%
  mutate(year = year - agri_year) %>%
  filter(year == 5) %>%
  group_by(agri_code, class_code) %>%
  summarise(count = n(), .groups = "drop") %>%
  collect()

trans_agg <- trans_time_serie %>%
  group_by(agri_code) %>%
  mutate(
    perc = count/sum(count, na.rm = TRUE) * 100,
    total = sum(count, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(total_perc = total/sum(count, na.rm = TRUE) * 100) %>%
  select(-c(count, total))

trans_agg %<>%
  ungroup() %>%
  inner_join(mb_dict, by = "agri_code") %>%
  mutate(agri_code = class_name) %>%
  select(-class_name) %>%
  inner_join(mb_dict, by = c("class_code" = "agri_code")) %>%
  mutate(class_code = class_name) %>%
  select(-class_name) %>%
  mutate(across(.cols = agri_code:class_code, ~ factor(.x)))

trans_agg %<>%
  mutate(
    across(
      .cols = agri_code:class_code,
      ~ fct_relevel(
        .x,
        "Soybean", "Sugar Cane", "Cotton", "Other Temporary Crops",
        "Other Perennial Crop", "Pasture", "Forest Formation",
        "Savanna Formation", "Grassland"
      )
    )
  ) %>%
  arrange(agri_code, class_code)

trans_agg %<>%
  pivot_wider(names_from = class_code, values_from = perc)

### Save plot data ----
write_csv(
  trans_time_serie,
  "data/figures/table_1.csv"
)

## Disconnect from Spark after all queries ----
spark_disconnect(sc)

# CREATE TABLE ----------------------------------------------------------------

trans_agg %>%
  select(1:11) %>%
  mutate(across(2:11, ~ round(.x, 2))) %>%
  relocate(total_perc) %>%
  gt()
