# HEADER ----------------------------------------------------------------------
#
# Title:        Merge raster tiles
# Description:  This script merges the raster tiles created in the
#               "03_rasterize.R" script into a single mosaic. Each mosaic is
#               created for each forest-agriculture transition cycle.
#
# Author:       Hugo Tameirao Seixas
# Contact:      tameirao.hugo@gmail.com
# Date:         2020-09-11
#
# Notes:        Do not change the names of files and directories of the
#               results, since it will break the next codes.
#
# LIBRARIES -------------------------------------------------------------------
#
library(gdalUtils)
library(raster)
library(fs)
library(magrittr)
library(glue)
library(tidyverse)
#
# OPTIONS ---------------------------------------------------------------------
#

#
# PREPARE FILES AND DIRECTORIES -----------------------------------------------

## Create directory to store new raster files ----
dir_create("data/trans_raster_mosaic/")

## List raster files with transition values ----
raster_list <- dir_ls("data/trans_raster_tiles/")

# LIST FOREST-AGRICULTURE TRANSITION CYCLES -----------------------------------

## Get number of cycles ----
raster_list %<>%
  as_tibble() %>%
  mutate(cycle = str_extract(raster_list, '(?<=cycle_)[^.tif]+')) %>%
  rename(path = value)

## Create list with the cycle numbers ----
cycle_list <- unique(raster_list$cycle)

# MERGE RASTER TILES ----------------------------------------------------------

## Merge raster for each cycle ----
walk(
  .x = cycle_list,
  function(cycle_num) {

    cat('Creating mosaic from cycle: ', c, '\n', sep = ' ')

    # Get the path for all raster of one cycle ----
    raster_path <- raster_list %>%
      filter(cycle == cycle_num) %>%
      pull(path)

    # Merge raster as a mosaic using GDAL ----
    raster_mosaic <- mosaic_rasters( # Creates a big raster dataset
      gdalfile = raster_path,
      dst_dataset = glue('data/trans_raster_mosaic/gdal_mosaic_cycle_{c}.vrt'),
      ot = 'Byte',
      srcnodata = 0,
      vrtnodata = 0,
      output_Raster = TRUE
    )

    cat('Saving file...', '\n')

    # Save mosaic as TIF file ----
    # (smaller than GDAL output)
    writeRaster(
      raster_mosaic,
      glue('data/trans_raster_mosaic/mb_mosaic_cycle_{c}.tif'),
      overwrite = TRUE,
      datatype = 'INT1U'
    )

    # Delete the virtual raster created by GDAL function ----
    file_delete(glue('data/trans_raster_mosaic/gdal_mosaic_cycle_{c}.vrt'))

  }
)

# Work complete!
try(beepr::beep(4), silent = TRUE)
