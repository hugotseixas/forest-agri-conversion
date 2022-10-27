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
library(sf)
library(geobr)
library(terra)
library(magrittr)
library(fs)
library(lubridate)
library(glue)
library(scales)
library(sparklyr)
library(ggridges)
library(dplyr)
library(tibble)
library(readr)
library(tidyr)
library(forcats)
library(ggplot2)
library(mapsf)
library(scico)
#
# OPTIONS ---------------------------------------------------------------------
#
source('conf/spark_config.R')
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
municip <- read_municipality(year = "2019")

amazon <- read_biomes() %>%
  filter(code_biome == 1) %>%
  st_transform("EPSG:4326")

# LOAD DATASETS ---------------------------------------------------------------

## Load transition length raster ----
mosaic <-
  rast("data/trans_raster_mosaic/mb_mosaic_cycle_1.tif")[["trans_length"]]

mosaic_agg <- terra::aggregate(mosaic, fact = 10, fun = "modal")

## Create hex grid ----
grid <- vect(st_make_grid(mosaic_agg, cellsize = 0.1, square = FALSE))

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

## Load trans_classes table ----
trans_classes <-
  spark_read_parquet(
    sc,
    name = "trans_classes",
    path = "data/trans_tabular_dataset/trans_classes/",
    memory = FALSE
  )

# FILTER AND AGGREGATE DATA ---------------------------------------------------

## Extract trans_length values from raster into hex grid ----
extract_values <- terra::extract(mosaic_agg, grid) %>%
  drop_na() %>%
  group_by(ID, trans_length) %>%
  count() %>%
  group_by(ID) %>%
  filter(n == max(n))

trans_hex <- grid %>%
  st_as_sf() %>%
  mutate(ID := seq_len(nrow(.))) %>%
  left_join(., extract_values, by = "ID") %>%
  filter(n > 20) %>%
  drop_na()

## Get the frequency of trans_lenght ----
frequency <- trans_length %>%
  select(trans_length) %>%
  sdf_sample(fraction = 0.01, replacement = FALSE, seed = 2) %>%
  collect()

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

### Mutate columns and convert to long format ----
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
  collect() %>%
  left_join(municip, by = "code_muni") %>%
  drop_na() %>%
  mutate(agri_year = ymd(agri_year + 1984, truncated = 2L))

## Disconnect from Spark after all queries ----
spark_disconnect(sc)

# CREATE PLOTS ----------------------------------------------------------------

## LULC percentage time series ----
pts <- trans_time_serie %>%
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
  scale_color_manual(values = palette, guide = "none") +
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
  )

ggsave(
  glue("./figs/trans_classes.png"),
  pts,
  width = 17,
  height = 18,
  units = "cm",
  dpi = 600
)

## Columns with transition years ----
cty <- trans_subset %>%
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
  geom_text(
    data = tibble(
      label = c("(a)", "(b)", "(c)", "(d)"),
      transition = as.factor(c("forest_year", "agri_year", "forest_year", "agri_year")),
      forest_type = c(1, 1, 2, 2),
      area = rep(5100, 4),
      year = rep(ymd("1985-06-01"), 4)
    ),
    aes(label = label, x = year, y = area),
    size = 3,
    color = "white"
  ) +
  scale_fill_scico(
    palette = "batlow",
    name = "Transition Length",
    breaks = c(1, 18, 36)
  ) +
  scale_color_scico(palette = "batlow", name = "Transition Length") +
  scale_x_date(
    date_breaks = "3 years",
    date_labels = "%Y",
    expand = c(0.02, 0.02)
  ) +
  scale_y_continuous(
    expand = c(0.02, 0.02),
    labels = scales::pretty_breaks(),
    breaks = pretty_breaks(3),
    limits = c(0, 4500)
  ) +
  labs(
    title = "Transition area per year and transition length",
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
  glue("./figs/trans_length_cols.png"),
  cty,
  width = 17,
  height = 14,
  units = "cm",
  dpi = 300
)

## Ridge plot ----
rp <- trans_ridges %>%
  filter(year(agri_year) >= 1995) %>%
  ggplot() +
  facet_wrap( ~ name_state, ncol = 2) +
  geom_density_ridges_gradient(
    aes(
      x = trans_length,
      y = agri_year,
      group = agri_year,
      fill = stat(x)
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
    breaks = c(1, 18, 36),
    limits = c(1, 36)
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
  glue("./figs/trans_ridge.png"),
  rp,
  width = 17,
  height = 15,
  units = "cm",
  dpi = 300
)

## Map ----

bbox <-
  st_as_sfc(
    st_bbox(
      c(
        xmin = -52.3262,
        xmax = -52.0786,
        ymax =  -12.3993,
        ymin =  -12.6592
      ),
      crs = st_crs(4326)
    )
  )

dist_map <- ggplot() +
  with_shadow(
    geom_sf(
      data = amazon,
      fill = "#747171",
      color = "transparent"
    ),
    sigma = 3,
    x_offset = 5,
    y_offset = 5
  ) +
  geom_sf(
    data = st_intersection(trans_hex, amazon),
    aes(fill = trans_length),
    lwd = 0.1
  ) +
  ggtitle(
    "Distribution of transitions from forest to agriculture in the Amazon"
  ) +
  guides(
    fill = guide_colourbar(
      barwidth = 0.5,
      barheight = 8,
      ticks = FALSE,
      frame.colour = "black"
    )
  ) +
  scale_fill_scico(
    palette = "batlow",
    breaks = c(1, 18, 36),
    name = "Transition Length (year)"
  ) +
  theme_void() +
  theme(
    legend.position = c(0.1, 0.77),
    text = element_text(size = 11)
  )

hr_map <- ggplot() +
  geom_raster(
    data = as.data.frame(crop(mosaic, bbox), xy = TRUE),
    aes(x = x, y = y, fill = trans_length)
  ) +
  scale_fill_scico(palette = "batlow") +
  scale_x_continuous(expand = c(0.0004, 0.0004)) +
  scale_y_continuous(expand = c(0.0004, 0.0004)) +
  coord_fixed() +
  theme_void() +
  theme(
    legend.position = "",
    panel.background = element_rect(fill = "#747171"),
    panel.border = element_rect(colour = "black", fill = NA, size = 0.5)
  )

hist_plot <- ggplot() +
  geom_bar(
    data = frequency,
    aes(x = trans_length, fill = factor(trans_length)),
    color = "black",
    lwd = 0.2
  ) +
  scale_fill_scico_d(palette = "batlow") +
  scale_x_continuous(expand = c(0, 0)) +
  scale_y_continuous(expand = c(0, 0)) +
  theme_void() +
  theme(
    legend.position = ""
  )

full_map <- dist_map +
  inset_element(hr_map, 0.76, 0, 1, 0.34, align_to = "panel") +
  inset_element(hist_plot, 0, 0, 0.5, 0.4, align_to = "panel")

ggsave(
  glue("./figs/map.png"),
  full_map,
  width = 17,
  height = 13,
  units = "cm",
  dpi = 300
)

