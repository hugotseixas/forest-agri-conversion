# HEADER ----------------------------------------------------------------------
#
# Title:        Analyze the forest-agriculture transition dataset
# Description:  The objective of this routine is to explore the dataset by
#               filtering and aggregating data in Spark. Results are supposed
#               to give us a overall view of the data by plots and tables.
#
# Author:       Hugo Tameirao Seixas
# Contact:      tameirao.hugo@gmail.com
# Date:         2020-09-11
#
# Notes:        In order to run this routine, you will need to successfully
#               install and connect to Spark using the {sparklyr} package.
#               Installation and use guide can be found in:
#               https://github.com/sparklyr/sparklyr
#
# LIBRARIES -------------------------------------------------------------------
#
library(magrittr)
library(fs)
library(lubridate)
library(glue)
library(scales)
library(sparklyr)
library(tidyverse)
#
# OPTIONS ---------------------------------------------------------------------
#
config <- spark_config()
config$`sparklyr.shell.driver-memory` <- "16G"
config$`sparklyr.shell.executor-memory` <- "8G"
config$`spark.yarn.executor.memoryOverhead` <- "1g"
#
# SET COLOR PALETTE AND LULC NAMES --------------------------------------------

## Get the names for each LULC class from MapBiomas ----
mb_dict <- read_csv("data/mb_class_dictionary.csv") %>%
  select(class_code, class_name) %>%
  rename(agri_code = class_code)

## Get the colors for each LULC class from MapBiomas
palette <- read_csv('data/mb_class_dictionary.csv')$class_color
names(palette) <- read_csv('data/mb_class_dictionary.csv')$class_code

# LOAD DATASET ----------------------------------------------------------------

## Connect with Spark -----
sc <- spark_connect(master = "local", config = config)

## Load trans_length table ----
trans_length <-
  spark_read_parquet(
    sc,
    name = "trans_length",
    path = "data/trans_tabular_dataset/trans_length/"
  )

## Load trans_classes table ----
trans_classes <-
  spark_read_parquet(
    sc,
    name = "trans_classes",
    path = "data/trans_tabular_dataset/trans_classes/"
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
  filter(!(class_code == 3 & year < agri_year)) %>%
  mutate(year = year - agri_year) %>%
  group_by(agri_code, forest_type, year, class_code) %>%
  summarise(count = n()) %>%
  mutate(perc = count/sum(count)) %>%
  collect()

## Count deforestation and agriculture establishment along years ----
trans_subset <-
  trans_length %>%
  filter(agri_cycle == 1) %>%
  select(cell_id:forest_type) %>%
  group_by(
    forest_type, agri_year, forest_year,
    agri_code, trans_length
  ) %>%
  summarise(count = n(), .groups = "keep") %>%
  collect()

## Mutate columns and convert to long format ----
trans_subset %<>%
  left_join(mb_dict, by = "agri_code") %>%
  mutate(across(c(agri_year, forest_year), ~.x + 1984L)) %>%
  gather(key = transition, value = year, agri_year:forest_year) %>%
  mutate(
    transition = fct_rev(factor(transition)),
    year = ymd(year, truncated = 2L)
  )

## Disconect from Spark after all queries ----
spark_disconnect(sc)

# CREATE PLOTS ----------------------------------------------------------------

## LULC percentage time serie ----
trans_time_serie %>%
  ggplot() +
  facet_grid(
    facets = agri_code ~ forest_type,
    labeller = labeller(
      agri_code = c(
        "20" = "Sugar Cane",
        "39" = "Soybean",
        "41" = "Other Temporary Crops"
      ),
      forest_type = c(
        "1" = "Primary Forest",
        "2" = "Secondary Forest"
      )
    )
  ) +
  geom_col(
    aes(
      x = factor(year),
      y = perc * 100,
      fill = factor(class_code),
      color = factor(class_code)
    )
  ) +
  geom_vline(xintercept = 33.5) +
  scale_fill_manual(values = palette) +
  scale_color_manual(values = palette, guide = FALSE) +
  scale_x_discrete(breaks = seq(-35, 5, 5), expand = c(0.02, 0.02)) +
  scale_y_continuous(limits = c(0, 100.1), expand = c(0.015, 0.015)) +
  labs(
    x = 'Years during and after transition',
    y = 'Percentage',
    fill = 'LULC Class'
  ) +
  theme_dark() +
  theme(
    text = element_text(size = 11),
    panel.grid = element_blank(),
    panel.border = element_blank(),
    legend.position = "bottom"
  ) +
  ggsave(
    glue("./plots/trans_classes.pdf"),
    width = 15,
    height = 18,
    units = "cm"
  )

## Columns with transition years for each agriculture type ----
walk(
  .x = pull(trans_subset %>% distinct(agri_code)),
  function(agri_class) {

    # Get the name of the agriculture class
    agri_name <-
      trans_subset %>%
      filter(agri_code == agri_class) %>%
      distinct(agri_code, class_name) %>%
      pull(class_name)

    # Create plot
    trans_subset %>%
      filter(agri_code == agri_class) %>%
      ggplot() +
      facet_grid(
        facets = transition ~ forest_type,
        labeller = labeller(
          transition = c(
            "forest_year" = "Deforestation",
            "agri_year" = "Agriculture establishment"
          ),
          forest_type = c(
            "1" = "Primary Forest",
            "2" = "Secondary Forest"
          )
        )
      ) +
      geom_col(
        aes(
          x = year,
          y = count,
          fill = trans_length,
          color = trans_length,
          group = trans_length
        )
      ) +
      scale_fill_continuous(type = "viridis", name = "Transition Length") +
      scale_color_continuous(type = "viridis", name = "Transition Length") +
      scale_x_date(
        date_breaks = "3 years",
        date_labels = "%Y",
        expand = c(0.02, 0.02)
      ) +
      scale_y_continuous(
        expand = c(0.02, 0.02),
        labels = scales::scientific,
        breaks = pretty_breaks(3)
      ) +
      theme_dark() +
      theme(
        text = element_text(size = 11),
        axis.text.y = element_text(angle = 45, vjust = 0.6, hjust = 0.5),
        axis.text.x = element_text(angle = 45, vjust = 0.6, hjust = 0.4),
        axis.title = element_blank(),
        legend.position = "bottom"
      ) +
      labs(title = glue("Transition length from Forest to {agri_name}")) +
      guides(
        fill = guide_colourbar(
          barwidth = 10,
          barheight = 0.8,
          title.vjust = 1
        ),
        color = FALSE
        ) +
      ggsave(
        glue("./plots/trans_length_{agri_class}.pdf"),
        width = 15,
        height = 12,
        units = "cm"
      )

  }
)
