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
#               results, since it will break the codes.
#
# LIBRARIES -------------------------------------------------------------------
#
library(gdalUtilities)
library(terra)
library(fs)
library(magrittr)
library(glue)
library(dplyr)
library(tibble)
library(purrr)
library(stringr)
#
# OPTIONS ---------------------------------------------------------------------
#

#
# PREPARE FILES AND DIRECTORIES -----------------------------------------------

## Create directory to store new raster files ----
dir_create("data/trans_raster_mosaic/")

## List raster files with transition values ----
file_list <- dir_ls("data/trans_raster_tiles/")

# LIST FOREST-AGRICULTURE TRANSITION CYCLES -----------------------------------

## Get number of cycles ----
file_list %<>%
  as_tibble() %>%
  mutate(cycle = str_extract(file_list, "(?<=cycle_)[^.tif]+")) %>%
  rename(path = value)

## Create list with the cycle numbers ----
cycle_list <- unique(file_list$cycle)

# MERGE RASTER TILES ----------------------------------------------------------

## Merge raster for each cycle ----
walk(
  .x = cycle_list,
  function(cycle_num) {

    cat("Creating mosaic from cycle: ", cycle_num, "\n", sep = " ")

    # Get the path for all raster of one cycle ----
    raster_path <-
      file_list %>%
      filter(cycle == cycle_num) %>%
      pull(path)

    # Merge raster as a mosaic using GDAL ----
    gdalbuildvrt(
      gdalfile = raster_path,
      glue("data/trans_raster_mosaic/gdal_mosaic_cycle_{cycle_num}.vrt"),
      hidenodata = FALSE,
      overwrite = TRUE
    )

    # Open virtual raster to SpatRaster object ----
    raster_mosaic <-
      rast(
        glue("data/trans_raster_mosaic/gdal_mosaic_cycle_{cycle_num}.vrt")
      )

    # Rename bands ----
    names(raster_mosaic) <- names(rast(raster_path[1]))

    cat('Saving file...', '\n')

    # Save mosaic as TIF file ----
    # (smaller than GDAL output)
    writeRaster(
      x = raster_mosaic,
      filename = glue(
        'data/trans_raster_mosaic/mb_mosaic_cycle_{cycle_num}.tif'
      ),
      overwrite = TRUE,
      wopt = list(
        datatype = "INT1U",
        progress = 0,
        gdal = c("COMPRESS=LZW")
      )
    )

    # Delete the virtual raster created by GDAL function ----
    file_delete(
      glue('data/trans_raster_mosaic/gdal_mosaic_cycle_{cycle_num}.vrt')
    )

  }
)

# Work complete!
try(beepr::beep(4), silent = TRUE)
