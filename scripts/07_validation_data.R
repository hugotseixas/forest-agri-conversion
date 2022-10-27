# HEADER ----------------------------------------------------------------------
#
# Title:        Download data to analyse the accuracy of results
# Description:  The objective of this routine is to download data to compare
#               the results of this project with satellite image compositions.
#
#
# Author:       Hugo Tameirao Seixas
# Contact:      tameirao.hugo@gmail.com
# Date:         2022-08-03
#
# Notes:
#
#
#
#
# LIBRARIES -------------------------------------------------------------------
#
library(rgee)
library(sf)
library(terra)
library(magrittr)
library(fs)
library(lubridate)
library(glue)
library(arrow)
library(dplyr)
library(units)
library(tibble)
library(readr)
library(tidyr)
library(forcats)
library(purrr)
library(ggplot2)
#
# OPTIONS ---------------------------------------------------------------------
#
source('conf/config.R')
#
# CREATE SAMPLES --------------------------------------------------------------

mask_cells <- open_dataset("data/trans_tabular_dataset/mask_cells/")

cells <- mask_cells %>%
  select(cell_id) %>%
  collect() %>%
  arrange(cell_id) %>%
  pull(cell_id)

set.seed(123)
sample_cells <- sample(cells, size = 100)

sample <- mask_cells %>%
  filter(cell_id %in% sample_cells) %>%
  collect() %>%
  st_as_sf(wkt = "coords", crs = "EPSG:4326") %>%
  select(-c(code_muni, area))

sample_buffer <-
  st_buffer(sample, set_units(1000, m))

dir_create("data/validation")

write_sf(
  sample,
  "data/validation/sample.fgb",
  delete_dsn = TRUE
)

write_sf(
  sample_buffer,
  "data/validation/sample_buffer.fgb",
  delete_dsn = TRUE
)

# DOWNLOAD IMAGES -------------------------------------------------------------

## Initialize GEE ----
ee_Initialize(
  user = gee_email, drive = TRUE
)

## Load MapBiomas collection 7 Landsat mosaic ----
mb_img <-
  ee$ImageCollection("projects/nexgenmap/MapBiomas2/LANDSAT/mosaics")

dir_create("data/validation/landsat")

export_bands <-
  c(
    "blue_median_dry", "blue_median_wet",
    "green_median_dry", "green_median_wet",
    "red_median_dry", "red_median_wet",
    "nir_median_dry", "nir_median_wet",
    "swir1_median_dry", "swir1_median_wet",
    "ndvi_amp"
  )

walk(
  .x = sample_buffer$cell_id,
  function(sample_cell) {

    dir_create(glue("data/validation/landsat/{sample_cell}"))

    aoi <-
      sample_buffer %>%
      filter(cell_id == sample_cell)

    aoi_ee <- sf_as_ee(aoi)

    mb_image_bounded <-
      mb_img$filterBounds(aoi_ee$geometry()$bounds())$
      select(export_bands)

    walk(
      .x = 1985:2020,
      function(year) {

        mb_image_year <- mb_image_bounded$
          filterMetadata("year", "equals", year)$
          mosaic()

        ee_as_raster(
          image = mb_image_year,
          region = aoi_ee$geometry()$bounds(),
          dsn = glue(
            "data/validation/landsat/{sample_cell}/{year}_{sample_cell}"
          ),
          scale = 30
        )

      }
    )

  }
)
