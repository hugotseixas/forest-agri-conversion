# HEADER ----------------------------------------------------------------------
#
# Title:        Analyze the forest-agriculture transition dataset
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
library(magrittr)
library(fs)
library(lubridate)
library(glue)
library(scales)
library(sparklyr)
library(ggridges)
library(tidyverse)
#
# OPTIONS ---------------------------------------------------------------------
#
source('conf/config.R')
#
# SET COLOR PALETTE AND LULC NAMES --------------------------------------------

## Get the names for each LULC class from MapBiomas ----
mb_dict <- read_csv("data/mb_class_dictionary.csv") %>%
  select(class_code, class_name) %>%
  rename(agri_code = class_code)

## Get the colors for each LULC class from MapBiomas
palette <- read_csv('data/mb_class_dictionary.csv')$class_color
names(palette) <- read_csv('data/mb_class_dictionary.csv')$class_code

# LOAD AUXILIARY DATA ---------------------------------------------------------

## Load municipality data ----
municip <- read_csv("data/municipalities_table.csv")

# LOAD DATASETS ---------------------------------------------------------------

## Connect with Spark -----
sc <- spark_connect(master = "local", config = config)

## Load mask_cells table ----
mask_cells <-
  spark_read_parquet(
    sc,
    name = "mask_cells",
    path = "data/trans_tabular_dataset/mask_cells/"
  )

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
  mutate(perc = count/sum(count, na.rm = TRUE)) %>%
  collect()

## Sum deforestation and agriculture establishment along years ----
trans_subset <-
  trans_length %>%
  filter(agri_cycle == 1) %>%
  select(cell_id:forest_type) %>%
  left_join(
    mask_cells %>% select(cell_id, area, code_muni),
    by = "cell_id"
  ) %>%
  group_by(
    forest_type, agri_year, forest_year,
    agri_code, trans_length, code_muni
  ) %>%
  summarise(total_area = sum(area, na.rm = TRUE)) %>%
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
  collect()

## Disconnect from Spark after all queries ----
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
    glue("./plots/trans_classes.png"),
    width = 15,
    height = 18,
    units = "cm",
    dpi = 600
  )

## Columns with transition years ----
trans_subset %>%
  group_by(transition, forest_type, trans_length, year) %>%
  summarise(total_area = sum(total_area, na.rm = TRUE), .groups = "drop") %>%
  ggplot() +
  facet_grid(
    facets = transition ~ forest_type,
    labeller = labeller(
      transition = c(
        "forest_year" = "Deforestation",
        "agri_year" = "Agriculture Establishment"
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
      y = total_area / 1e6,
      fill = trans_length,
      color = trans_length,
      group = trans_length
    ),
    size = 0.25
  ) +
  scale_fill_viridis_c(option = "C", name = "Transition Length") +
  scale_color_viridis_c(option = "C", name = "Transition Length") +
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
  guides(
    fill = guide_colourbar(
      barwidth = 10,
      barheight = 0.8,
      title.vjust = 1
    ),
    color = FALSE
  ) +
  ggsave(
    glue("./plots/trans_length_cols.png"),
    width = 15,
    height = 12,
    units = "cm",
    dpi = 600
  )

## Ridge plot ----
trans_ridges %>%
  left_join(municip, by = "code_muni") %>%
  drop_na() %>%
  mutate(agri_year = ymd(agri_year + 1984, truncated = 2L)) %>%
  filter(
    trans_length <= 10,
    year(agri_year) >= 1995
  ) %>%
  ggplot() +
  facet_wrap( ~ name_state, ncol = 2) +
  geom_density_ridges_gradient(
    aes(
      x = trans_length,
      y = agri_year,
      group = agri_year,
      fill = stat(x)
    ),
    bandwidth = 1
  ) +
  scale_fill_viridis_c(option = "C") +
  scale_y_date(
    date_breaks = "3 years",
    date_labels = "%Y"
  ) +
  theme_dark() +
  coord_flip() +
  labs(
    x = "Transition Length",
    y = "Agriculture Establishment",
    fill = "Transition Length"
  ) +
  guides(
    fill = guide_colourbar(
      barwidth = 10,
      barheight = 0.8,
      title.vjust = 1
    )
  ) +
  theme(
    text = element_text(size = 11),
    axis.text.x = element_text(angle = 45, vjust = 0.6, hjust = 0.4),
    legend.position = "bottom"
  ) +
  ggsave(
    glue("./plots/trans_ridge.png"),
    width = 15,
    height = 15,
    units = "cm",
    dpi = 600
  )
