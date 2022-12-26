# HEADER ----------------------------------------------------------------------
#
# Title:        Create figure 5
# Description:  The objective of this routine is to explore the dataset by
#               filtering and aggregating data in Spark. Results are supposed
#               to give us a overall view of the data by plots and tables.
#
# Author:       Hugo Tameirao Seixas
# Contact:      seixas.hugo@protonmail.com
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

## Load mask_cells table ----
mask_cells <-
  spark_read_parquet(
    sc,
    name = "mask_cells",
    path = "data/c_tabular_dataset/mask_cells/",
    memory = FALSE
  )

## Load c_length table ----
c_length <-
  spark_read_parquet(
    sc,
    name = "c_length",
    path = "data/c_tabular_dataset/c_length/",
    memory = FALSE
  )

# FILTER AND AGGREGATE DATA ---------------------------------------------------

## Sum deforestation and agriculture establishment along years ----
c_subset <-
  c_length %>%
  filter(agri_cycle == 1) %>%
  select(cell_id:forest_type) %>%
  left_join(
    mask_cells %>% select(cell_id, area),
    by = "cell_id"
  ) %>%
  group_by(
    forest_type, agri_year, forest_year,
    agri_code, c_length
  ) %>%
  summarise(total_area = sum(area, na.rm = TRUE), .groups = "drop") %>%
  collect()

### Mutate columns and convert to long format ----
c_subset %<>%
  left_join(mb_dict, by = "agri_code") %>%
  pivot_longer(
    agri_year:forest_year,
    names_to = "conversion",
    values_to = "year") %>%
  mutate(
    conversion = fct_rev(factor(conversion)),
    year = ymd(year, truncated = 2L),
    forest_type = as.character(forest_type)
  )

### Summarise data ----
c_subset %<>%
  group_by(conversion, forest_type, c_length, year) %>%
  summarise(total_area = sum(total_area, na.rm = TRUE), .groups = "drop") %>%
  mutate(total_area = total_area / 1e6)

### Save plot data ----
write_csv(
  c_subset,
  "data/figures/fig_5.csv"
)

## Disconnect from Spark after all queries ----
spark_disconnect(sc)

# CREATE PLOTS ----------------------------------------------------------------

## Columns with conversion years ----
cty <- c_subset %>%
  ggplot() +
  facet_grid(
    facets = conversion ~ forest_type,
    labeller = labeller(
      conversion = c(
        "forest_year" = "Deforestation",
        "agri_year" = "Agriculture Establishment"
      ),
      forest_type = c(
        "1" = "Primary Forest",
        "2" = "Secondary Forest"
      )
    )
  ) +
  geom_vline(
    xintercept = ymd("2004-01-01", "2006-01-01", "2012-01-01"),
    color = "white",
    linetype = "dashed"
  ) +
  geom_richtext(
    data = tibble(
      year = ymd("2004-05-01", "2006-05-01", "2012-05-01"),
      total_area = c(5000, 3500, 2000),
      forest_type = factor(1),
      conversion = factor("forest_year"),
      label = c("PPCDAm", "Soy Moratorium", "Forest Code")
    ),
    aes(x = year, y = total_area, label = label),
    size = 2.5,
    hjust = "left",
    label.r = unit(0, "pt"),
    label.padding = unit(2, "pt"),
    fill = "#dddddd"
  ) +
  geom_col(
    aes(
      x = year,
      y = total_area,
      fill = c_length,
      color = c_length,
      group = c_length
    ),
    lwd = 0.1
  ) +
  geom_col(
    data = . %>%
      group_by(conversion, forest_type, year) %>%
      summarise(total_area = sum(total_area, na.rm = TRUE), .groups = "drop"),
    aes(
      x = year,
      y = total_area
    ),
    fill = "transparent",
    color = "black",
    lwd = 0.2
  ) +
  geom_text(
    data = tibble(
      label = c("(a)", "(b)", "(c)", "(d)"),
      conversion =
        as.factor(c("forest_year", "agri_year", "forest_year", "agri_year")),
      forest_type = c("1", "1", "2", "2"),
      area = rep(5100, 4),
      year = rep(ymd("1986-01-01"), 4)
    ),
    aes(label = label, x = year, y = area),
    size = 3,
    color = "white"
  ) +
  scale_fill_scico(
    palette = "batlow",
    name = "Conversion Length (year)",
    breaks = c(0, 17, 35)
  ) +
  scale_color_scico(palette = "batlow", name = "Conversion Length (year)") +
  scale_x_date(
    date_breaks = "3 years",
    date_labels = "%Y",
    expand = c(0.02, 0.02)
  ) +
  scale_y_continuous(
    expand = c(0.02, 0.02),
    breaks = pretty_breaks(3),
  ) +
  labs(
    title = "Conversion area per year and conversion length",
    x = "Year",
    y = "Area (square kilometre)"
  ) +
  theme_dark() +
  theme(
    text = element_text(size = 11),
    axis.text.y = element_text(angle = 45, vjust = 0.6, hjust = 0.5),
    axis.text.x = element_text(angle = 45, vjust = 0.6, hjust = 0.4),
    legend.position = "bottom"
  ) +
  guides(
    fill = guide_colourbar(
      title.vjust = 1,
      barwidth = 10,
      barheight = 0.5,
      ticks = FALSE,
      frame.colour = "black"
    ),
    color = "none"
  )

ggsave(
  "./figs/c_length_cols.png",
  cty,
  width = 17,
  height = 14,
  units = "cm",
  dpi = 300
)
