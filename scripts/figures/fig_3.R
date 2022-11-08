# HEADER ----------------------------------------------------------------------
#
# Title:        Create figure 3
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
library(sf)
library(geobr)
library(magrittr)
library(lubridate)
library(scales)
library(sparklyr)
library(ggridges)
library(dplyr)
library(tibble)
library(readr)
library(tidyr)
library(forcats)
library(ggplot2)
library(scico)
#
# OPTIONS ---------------------------------------------------------------------
#
source('conf/spark_config.R')
#
# LOAD AUXILIARY DATA ---------------------------------------------------------

## Load municipality data ----
municip <- read_municipality(year = "2019")

# LOAD DATASETS ---------------------------------------------------------------

## Connect with Spark -----
sc <- spark_connect(master = "local", config = config, version = "3.3.0")

## Load mask_cells table ----
mask_cells <-
  spark_read_parquet(
    sc,
    name = "mask_cells",
    path = "data/trans_tabular_dataset/mask_cells/",
    memory = FALSE
  )

## Load trans_length table ----
trans_length <-
  spark_read_parquet(
    sc,
    name = "trans_length",
    path = "data/trans_tabular_dataset/trans_length/",
    memory = FALSE
  )

# FILTER AND AGGREGATE DATA ---------------------------------------------------

## Sample rows to create ridge plot ----
trans_ridges <-
  trans_length %>%
  filter(agri_cycle == 1) %>%
  select(cell_id:forest_type) %>%
  sdf_sample(fraction = 0.2, replacement = FALSE) %>%
  left_join(
    mask_cells %>% select(cell_id, area, code_muni),
    by = "cell_id"
  ) %>%
  collect() %>%
  left_join(municip, by = "code_muni") %>%
  drop_na() %>%
  mutate(agri_year = ymd(agri_year, truncated = 2L)) %>%
  filter(year(agri_year) >= 1995)

## Disconnect from Spark after all queries ----
spark_disconnect(sc)

# CREATE PLOTS ----------------------------------------------------------------

## Ridge plot ----
rp <- trans_ridges %>%
  ggplot() +
  facet_wrap( ~ name_state, ncol = 2) +
  geom_density_ridges_gradient(
    aes(
      x = trans_length,
      y = agri_year,
      group = agri_year,
      fill = after_stat(x)
    ),
    bandwidth = 2
  ) +
  geom_text(
    data = tibble(
      label = c("(j)", "(g)", "(b)", "(i)", "(h)", "(e)", "(a)", "(d)", "(c)"),
      name_state = unique(trans_ridges$name_state),
      trans_length = rep(34, 9),
      agri_year = rep(ymd("1995-01-01"), 9)
    ),
    aes(x = trans_length, y = agri_year, label = label),
    size = 3,
    color = "white"
  ) +
  scale_fill_scico(
    palette = "batlow",
    breaks = c(0, 17, 35),
    limits = c(0, 35)
  ) +
  scale_x_continuous(breaks = pretty_breaks(n = 4), limits = c(1, 36)) +
  scale_y_date(
    date_breaks = "2 years",
    date_labels = "%Y"
  ) +
  theme_dark() +
  coord_flip() +
  labs(
    title = "Transition length patterns inside states",
    x = "Transition Length",
    y = "Agriculture Establishment",
    fill = "Transition Length"
  ) +
  guides(
    fill = guide_colourbar(
      title.vjust = 1,
      barwidth = 10,
      barheight = 0.5,
      ticks = FALSE,
      frame.colour = "black"
    )
  ) +
  theme(
    text = element_text(size = 11),
    axis.text.x = element_text(angle = 45, vjust = 0.6, hjust = 0.4),
    legend.position = "bottom"
  )

ggsave(
  "./figs/trans_ridge.png",
  rp,
  width = 17,
  height = 15,
  units = "cm",
  dpi = 300
)
