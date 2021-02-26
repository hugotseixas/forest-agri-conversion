# HEADER ----------------------------------------------------------------------
#
# Title:        Create transition raster files
# Description:  This codes takes values from the "trans_length" dataset and
#               metadata information to recreate raster tiles with the
#               transition values. The new raster files share the same spatial
#               characteristics as the downloaded tiles (extent, resolution...)
#               Each tile will be composed by a set of raster files, one file
#               for each forest-agriculture transition cycle, and the bands of
#               the new files carry values of the variables stored in the
#               parquet dataset.
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
library(terra)
library(magrittr)
library(fs)
library(glue)
library(arrow)
library(tibble)
library(dplyr)
library(purrr)
#
# OPTIONS ---------------------------------------------------------------------
#

#
# GET TILES LIST --------------------------------------------------------------

## Get tiles list from metadata file ----
tiles_metadata <-
  read_parquet("data/trans_tabular_dataset/tiles_metadata.parquet")

# RASTERIZE TABLES ------------------------------------------------------------

## Create directory to store new raster files  ----
dir_create("data/trans_raster_tiles/")

## Load transition_length values from parquet dataset  ----
trans_ds <- open_dataset("data/trans_tabular_dataset/trans_length/")

## Load mask cells from parquet dataset  ----
mask_ds <- open_dataset("data/trans_tabular_dataset/mask_cells/")

## Create set of raster files for each tile ----
walk(
  .x = tiles_metadata$tile_id,
  function(tile) {

    cat('\n')

    cat('Creating raster from tile:', tile, '\n', sep = ' ')

    # Get cells from mask for the tile ----
    cells <-
      mask_ds %>%
      filter(tile_id == tile) %>%
      select(cell_id, tile_cell_id) %>%
      collect()

    # Get metadata for the tile ----
    meta <-
      tiles_metadata %>%
      filter(tile_id == tile)

    # Create one raster for each group (agri_cycle) ----
    trans_ds %>%
      filter(tile_id == tile) %>% # First filter the dataset for the tile
      select(-tile_id) %>%
      group_by(agri_cycle) %>%
      collect() %>%
      group_walk(
        ~ {

          ##### Set values for all raster cells ----
          trans_table <-
            .x %>%
            # Get cell id of the tile
            full_join(cells, by = "cell_id") %>%
            # Expand values to the whole raster extent
            # Will include NA values for empty cells (very important)
            right_join(
              tibble(tile_cell_id = c(1:meta$n_cell)),
              by = "tile_cell_id"
            ) %>%
            arrange(tile_cell_id)

          # Get variables names ----
          variables <-
            trans_table %>%
            select(agri_code:forest_type) %>%
            names()

          # Create a list of raster layers and their values ----
          trans_raster_list <-
            map(
              .x = variables,
              function(var) {

                #### Create empty raster using metadata values ----
                trans_raster <-
                  rast(
                    extent = ext(
                      c(meta$x_min, meta$x_max, meta$y_min, meta$y_max)
                    ),
                    ncols = meta$n_col,
                    nrows = meta$n_row,
                    crs = meta$crs
                  )

                # Arrange cell values ----
                raster_values <-
                  trans_table %>%
                  arrange(tile_cell_id) %>%
                  pull(!!var)

                # Fill layer with values ----
                trans_raster <- setValues(trans_raster, values = raster_values)

                # Set layer name ----
                names(trans_raster) <- var

                return(trans_raster)

              }
            )

          # Create raster stack from list ----
          trans_stack <- rast(trans_raster_list)

          # Save stack to file ----
          writeRaster(
            trans_stack,
            glue(
              'data/trans_raster_tiles/',
              'trans_stack_{meta$tile_code}_cycle_{.y}.tif'
            ),
            wopt = list(
              datatype = 'INT1U',
              progress = 0
            ),
            overwrite = TRUE
          )

        }
      )

  }
)

# Work complete!
try(beepr::beep(4), silent = TRUE)
