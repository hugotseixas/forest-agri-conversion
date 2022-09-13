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
#               Check the "conf/config.R" file to change options for this
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
library(geojsonio)
library(googledrive)
library(sf)
library(geobr)
library(glue)
library(fs)
library(dplyr)
library(purrr)
#
# OPTIONS ---------------------------------------------------------------------
#
source('conf/config.R')
options(googledrive_quiet = TRUE)
#
# START GEE API ---------------------------------------------------------------

## Initialize GEE ----
ee_Initialize(
  user = gee_email, drive = TRUE
)

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

## Load MapBiomas collection 6 ----
mb_img <-
  ee$Image(
    paste(
      'projects',
      'mapbiomas-workspace',
      'public',
      'collection7',
      'mapbiomas_collection70_integration_v2',
      sep = '/'
    )
  )

## Change product scale if it is not the default value ----
if (scale != 30) {

  mb_crs <- mb_img$projection()

  mb_img <-
    mb_img$
    reduceResolution(reducer = ee$Reducer$mode(), maxPixels = 1000)$
    reproject(crs = mb_crs, scale = scale)

}

## Get band names ----
bands <- mb_img$bandNames()$getInfo()

# CREATE AND APPLY MASKS ------------------------------------------------------

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
  reduce(ee$Reducer$anyNonZero())

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
  reduce(ee$Reducer$anyNonZero())

## Calculate the area of each pixel of the masks ----
mb_mask <-
  mb_img$
  pixelArea()$
  updateMask(a_mask)$
  updateMask(f_mask)

## Apply masks ----
mb_img <- mb_img$updateMask(a_mask)$updateMask(f_mask)

## View mask if allowed ----
if (view_map == TRUE) {

  Map$centerObject(aoi)

  Map$addLayer(
    eeObject = mb_mask$clip(aoi),
    visParams = list(
      bands = c("area"),
      min = 1 # Avoid showing pixels in white
    ),
    name = "MapBiomas Mask"
  )

}

# DOWNLOAD DATA TO DRIVE ------------------------------------------------------

## Create a time code to add to folder names ----
time_code <- as.integer(Sys.time())

## Create folders ----

# Parent folder
drive_mkdir(
  name = glue("mb_transition-{time_code}"),
  path = "~/",
  overwrite = FALSE
)

# Sub folders
walk(
  .x = c("mb_mask", "mb_lulc"),
  function(folder_name) {

    drive_mkdir(
      name = glue("{folder_name}-{time_code}"),
      path = glue("~/mb_transition-{time_code}/"),
      overwrite = FALSE
    )

  }
)

## Set download task for mask data ----
download_mask <-
  ee_image_to_drive(
    image = mb_mask$clip(aoi),
    description = "mb_mask",
    folder = glue("mb_mask-{time_code}"),
    timePrefix = FALSE,
    region = aoi$geometry()$bounds(),
    scale = scale,
    maxPixels = 1e13,
    fileDimensions = tile_dim,
    skipEmptyTiles = TRUE,
    fileFormat = "GEO_TIFF",
    crs = "EPSG:4326"
  )

## Set download task for MapBiomas data ----
download_mb <-
  ee_image_to_drive(
    image = mb_img$clip(aoi),
    description = "mb_lulc",
    folder = glue("mb_lulc-{time_code}"),
    timePrefix = FALSE,
    region = aoi$geometry()$bounds(),
    scale = scale,
    maxPixels = 1e13,
    fileDimensions = tile_dim,
    skipEmptyTiles = TRUE,
    fileFormat = "GEO_TIFF",
    crs = "EPSG:4326"
  )

### Start download tasks ----
download_mask$start()
download_mb$start()

### Monitor downloads ----
ee_monitoring(download_mask, task_time = 600)
ee_monitoring(download_mb, task_time = 600)

# DOWNLOAD FROM DRIVE TO LOCAL DISK -------------------------------------------

## List files ----
# Replicate file search 10 times and get distinct values
# Tries to avoid a problem where drive_ls does not find all files
# This is not optimal and should be replaced as soon as possible
drive_files <-
  map_dfr(
    1:10,
    ~ drive_ls(glue("~/mb_transition-{time_code}/"), recursive = TRUE)
  ) %>%
  distinct(name, .keep_all = TRUE) %>%
  filter(
    !name %in% c(glue("mb_lulc-{time_code}"), glue("mb_mask-{time_code}"))
  )

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
