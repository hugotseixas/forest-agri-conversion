# HEADER ----------------------------------------------------------------------
#
# Title:        Download MapBiomas raster
# Description:  This code uses the Google Earth Engine API within R to access
#               and download Land Use and Land Cover (LULC) data in the
#               Brazilian Amazon biome. The LULC data is provided by the
#               MapBiomas project:
#               (https://mapbiomas.org/en?cama_set_language=en)
#
# Author:       Hugo Tameirao Seixas
# Contact:      tameirao.hugo@gmail.com
# Date:         2020-09-11
#
# Notes:        In order to run this routine, you will need to have access
#               to Google Earth Engine (https://earthengine.google.com/).
#               You will also need to install and configure the {rgee} package,
#               Installation and use guide can be found in:
#               https://r-spatial.github.io/rgee/
#
#               Check the "00_config.R" file to change options for this
#               routine.
#
#               Avoid opening the raster tiles in any GIS, since they may
#               create auxiliary files that can break the next codes.
#               If you want to open these files in a GIS, please copy and
#               paste the files into a different directory outside this
#               project.
#
# LIBRARIES -------------------------------------------------------------------
#
library(rgee)
library(googledrive)
library(sf)
library(geobr)
library(glue)
library(fs)
library(tidyverse)
#
# OPTIONS ---------------------------------------------------------------------
#
source('00_config.R')
#
# START GEE API ---------------------------------------------------------------

## Initialize GEE ----
ee_Initialize(email = gee_email, drive = TRUE)

# SET REGION OF INTEREST ------------------------------------------------------

## Load biome limits data ----
biomes <-
  read_biomes(simplified = TRUE) %>%
  filter(code_biome == biome)

cat('Download MapBiomas data for', biomes$name_biome)

## Set region of interest in GEE ----
aoi <- sf_as_ee(biomes)
# Can take some minutes to import the polygon

# LOAD LULC DATA --------------------------------------------------------------

## Load MapBiomas collection 5 ----
mb_img <-
  ee$Image(
    paste(
      'projects',
      'mapbiomas-workspace',
      'public',
      'collection5',
      'mapbiomas_collection50_integration_v1',
      sep = '/'
    )
  )$
  byte()

## Get band names ----
bands <- mb_img$bandNames()$getInfo()

# CREATE AND APPLY MASKS ------------------------------------------------------

## Create water mask ----
w_mask <-
  ee$Image("JRC/GSW1_1/GlobalSurfaceWater")$
  select("max_extent")$
  remap(c(0,1), c(1,0))$
  byte()

## Create agriculture mask ----
a_mask <-
  ee$ImageCollection$fromImages(
  map(bands, function(band_name) {
    values <- c(trans_ant, -1) # The "-1" avoids an error with ee.List())
    return(mb_img$
      select(band_name)$
      remap(
        from = values,
        to = rep(1, length(values)),
        defaultValue = 0
        )
      )
    })
  )$
  reduce(ee$Reducer$countDistinct())$
  # Turn to 1 pixels that had other classes besides the mask along time series
  remap(from = c(1, 2), to = c(0, 1), defaultValue = 0)

## Create forest mask ----
f_mask <-
  ee$ImageCollection$fromImages(
    map(bands, function(band_name) {
      values <- c(trans_nat, -1) # The "-1" avoids an error with ee.List())
      return(mb_img$
        select(band_name)$
        remap(
          from = values,
          to = rep(1, length(values)),
          defaultValue = 0
        )
      )
    })
  )$
  reduce(ee$Reducer$countDistinct())$
  remap(from = c(1, 2), to = c(0, 1), defaultValue = 0)

## Combine masks ----
mb_mask <- w_mask$updateMask(a_mask)$updateMask(f_mask)

## Apply masks ----
mb_img <- mb_img$updateMask(mb_mask)

##-- View mask if allowed ----
if(view_map == TRUE) {

  Map$centerObject(aoi)

  Map$addLayer(mb_mask$clip(aoi), name = "MapBiomas Mask")

}

# DOWNLOAD DATA TO DRIVE ------------------------------------------------------

## Delete googledrive download folder if allowed ----
if (clear_driver_folder == TRUE) { drive_rm('mb_transition') }

## Set download task for mask data ----
download_mask <-
  ee_image_to_drive(
    image = mb_mask$clip(aoi),
    description = "mb_mask",
    folder = "mb_transition",
    timePrefix = FALSE,
    region = aoi$geometry()$bounds(),
    scale = scale,
    maxPixels = 1e13,
    fileDimensions = tile_dim,
    skipEmptyTiles = TRUE,
    fileFormat = "GEO_TIFF"
  )

### Start download ----
download_mask$start()

### Monitor the download ----
ee_monitoring(download_mask, task_time = 60)

## Set download task for MapBiomas data ----
download_mb <-
  ee_image_to_drive(
    image = mb_img$clip(aoi),
    description = "mb_lulc",
    folder = "mb_transition",
    timePrefix = FALSE,
    region = aoi$geometry()$bounds(),
    scale = scale,
    maxPixels = 1e13,
    fileDimensions = tile_dim,
    skipEmptyTiles = TRUE,
    fileFormat = "GEO_TIFF"
  )

### Start download ----
download_mb$start()

### Monitor the download ----
ee_monitoring(download_mb, task_time = 60)

# DOWNLOAD FROM DRIVE TO LOCAL DISK -------------------------------------------

## List files ----
drive_files <- drive_ls('/mb_transition')

## Create local download dir ----
dir_create("data/raw_raster_tiles")

## Download files to project folder ----
walk2(
  .x = drive_files$id,
  .y = drive_files$name,
  function(id, name) {

    drive_download(
    file = as_id(id),
    path = glue::glue('data/raw_raster_tiles/{name}'),
    overwrite = TRUE
    )

  }
)

# Work complete!
try(beepr::beep(4), silent = TRUE)
