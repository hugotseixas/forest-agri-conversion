# HEADER ----------------------------------------------------------------------
#
# Title:        Create figure 1
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
library(sparklyr)
library(dplyr)
library(tibble)
library(readr)
library(tidyr)
library(forcats)
library(ggplot2)
library(ggfx)
library(patchwork)
library(scico)
#
# OPTIONS ---------------------------------------------------------------------
#
source('conf/spark_config.R')
#
# LOAD AUXILIARY DATA ---------------------------------------------------------

amazon <- read_biomes() %>%
  filter(code_biome == 1) %>%
  st_transform("EPSG:4326")

states <- read_state() %>%
  st_transform("EPSG:4326") %>%
  st_intersection(amazon)

# LOAD DATASETS ---------------------------------------------------------------

## Load transition length raster ----
mosaic <-
  rast("data/trans_raster_mosaic/mb_mosaic_cycle_1.tif")[["trans_length"]]

mosaic_agg <- terra::aggregate(mosaic, fact = 10, fun = "modal")

## Create hex grid ----
grid <- vect(st_make_grid(mosaic_agg, cellsize = 0.1, square = FALSE))

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
  drop_na() %>%
  st_intersection(amazon)

## Crop raster to extract full resolution values ----
hr_bbox <-
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

hr_raster <-
  as.data.frame(
    crop(
      mosaic,
      hr_bbox
    ),
    xy = TRUE
  ) %>%
  as_tibble()

## Get the frequency of trans_lenght ----
frequency <- trans_length %>%
  select(trans_length) %>%
  sdf_sample(fraction = 0.01, replacement = FALSE, seed = 2) %>%
  collect()

## Save plot data ----
# Hex grid data
write_csv(trans_hex, "data/figures/fig_1_hex.csv")

# Transition length frequency
write_csv(frequency, "data/figures/fig_1_freq.csv")

# High resolution raster values
write_csv(hr_raster, "data/figures/fig_1_hr.csv")

## Disconnect from Spark after all queries ----
spark_disconnect(sc)

# CREATE PLOTS ----------------------------------------------------------------

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
    data = states,
    fill = "transparent",
    color = "black"
  ) +
  geom_sf(
    data = trans_hex,
    aes(fill = trans_length),
    linewidth = 0.05
  ) +
  geom_sf(
    data = hr_bbox,
    fill = "transparent",
    color = "#c50505"
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
    breaks = c(0, 17, 35),
    name = "Transition Length (year)"
  ) +
  theme_void() +
  theme(
    legend.position = c(0.14, 0.77),
    text = element_text(size = 11)
  )

hr_map <- ggplot() +
  geom_raster(
    data = hr_raster,
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
    panel.border = element_rect(colour = "black", fill = NA, linewidth = 0.5)
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

full_map <- dist_map + theme(plot.tag.position = c(0.2, 0.8)) +
  inset_element(hr_map, 0.76, 0, 1, 0.34, align_to = "panel") +
  theme(plot.tag.position = c(0.95, 1.08)) +
  inset_element(hist_plot, 0, 0, 0.5, 0.4, align_to = "panel") +
  theme(plot.tag.position = c(0.4, 0.4)) +
  plot_annotation(tag_levels = 'a')

ggsave(
  "./figs/map.png",
  full_map,
  width = 17,
  height = 12.99,
  units = "cm",
  dpi = 300
)

