#
# Title:        Extract mask
# Description:  This code extracts area values of the mask raster files and
#               its metadata. The process is applied in each file. The results
#               are saved in .parquet format.
#
# Author:       Hugo Tameirao Seixas
# Contact:      tameirao.hugo@gmail.com
# Date:         2020-09-11
#
# Notes:        Do not change the names of files and directories of the
#               results, since it may break the next codes.
#
# LIBRARIES -------------------------------------------------------------------
#
library(sf)
library(terra)
library(geobr)
library(fs)
library(magrittr)
library(glue)
library(arrow)
library(readr)
library(tidyr)
library(stringr)
library(tibble)
library(purrr)
library(dplyr)
#
# OPTIONS ---------------------------------------------------------------------
#
source("conf/config.R")
source("R/terra_as_tibble.R")
sf::sf_use_s2(TRUE)
#
# LOAD AUXILIARY DATA ---------------------------------------------------------

## Load biome polygon ----
biomes <-
  read_biomes(simplified = TRUE) %>%
  filter(code_biome == biome)

## Load municipalities vector data ----
municip <-
  read_municipality(year = "2019") %>%
  st_join(biomes, join = st_intersects) %>%
  filter(code_biome == biome) %>%
  select(code_muni, geom)

# LIST RASTER FILES -----------------------------------------------------------

## Create a list of files for each raster type ----
file_list <-
  dir_info(
    path = 'data/raw_raster_tiles/',
    regexp = "mb_mask.+tif$"
  ) %>%
  select(path) %>%
  mutate(
    tile_code = str_extract(path, "(?<=mb_mask-)[^.tif]+")
  ) %>%
  mutate(tile_id = row_number()) %>%
  relocate(tile_id, tile_code) %>%
  arrange(tile_code)

# EXTRACT RASTER METADATA ----

## Extract metadata of each raster tile ----
tiles_metadata <-
  map2_df(
    .x = pull(.data = file_list, var = path),
    .y = pull(.data = file_list, var = tile_id),
    function(raster_path, tile) {

      # Load one file as a raster stack ----
      raster <- rast(raster_path)

      ## Add row to raster metadata table ----
      tiles_metadata <-
        tibble(
          tile_id = tile,
          tile_code = file_list$tile_code[tile],
          x_min = xmin(raster),
          x_max = xmax(raster),
          y_min = ymin(raster),
          y_max = ymax(raster),
          n_col = ncol(raster),
          n_row = nrow(raster),
          n_cell = ncell(raster),
          crs = as.character(crs(raster, proj4 = TRUE))
        )

    }
  )

# EXTRACT MASK VALUES ---------------------------------------------------------

## Extract mask values into a single table ----
mask_table <-
  map2_df(
    .x = pull(.data = file_list, var = path),
    .y = pull(.data = file_list, var = tile_id),
    function(raster_path, tile) {

      # Load one file as a raster stack ----
      raster <- rast(raster_path)

      cat('\n')

      cat(
        'Extracting from tile:',
        tile,
        '-',
        raster_path,
        '\n',
        sep = ' '
      )

      # Extract values of a single tile ----
      mask_subset <-
        raster %>%
        terra_as_tibble(xy = TRUE, cell = TRUE) %>%
        rename(tile_cell_id = cellindex) %>%
        filter(!(cellvalue == 0)) %>% # Remove empty cells
        mutate(tile_id = tile) %>%
        rename(area = cellvalue)

      # Check if there is any valid pixel ----
      # Avoids possible unexpected errors
      if (nrow(mask_subset) == 0) { return() }

      # Add municipality in which each pixel is within ----
      mask_subset <-
        mask_subset %>%
        st_as_sf(coords = c("x", "y"), crs = crs(raster)) %>%
        st_join(
          municip %>% st_transform(crs(raster)),
          join = st_nearest_feature # Avoids duplicates
          # It may not join the geometry in which the point is inside
        ) %>%
        mutate(coords = st_as_text(geometry)) %>%
        as_tibble() %>%
        select(-geometry)

      return(mask_subset)

    }
  )

## Create unique cell_id for the whole mask ----
mask_table %<>%
  mutate(cell_id = row_number()) %>%
  relocate(cell_id, tile_id, tile_cell_id)

## Create directories and store tables as parquet files ----
walk(
  .x = pull(.data = file_list, var = tile_id),
  function(tile) {

    ## Filter rows of a tile ----
    table <-
      mask_table %>%
      filter(tile_id == tile) %>%
      select(-tile_id)

    # Check if table have any row
    if (nrow(table) > 0) {

      ## Create directories ----
      dir_create(
        path = glue(
          "data/trans_tabular_dataset/mask_cells/",
          "tile_id={tile}/"
        )
      )

      # Write files ----
      write_parquet(
        x = table,
        sink = glue(
          "data/trans_tabular_dataset/mask_cells/",
          "tile_id={tile}/",
          "mask_cells-{file_list$tile_code[tile]}.parquet"
        ),
        version = "2.0"
      )

    }

  }
)

## Save raster metadata file ----
write_parquet(
  x = tiles_metadata,
  sink = "data/trans_tabular_dataset/tiles_metadata.parquet",
  version = "2.0"
)

# Work complete!
try(beepr::beep(4), silent = TRUE)

