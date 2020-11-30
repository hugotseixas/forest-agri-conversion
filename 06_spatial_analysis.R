# HEADER ----------------------------------------------------------------------
#
# Title:        Explore spatial aggregations of the transition data
# Description:  The objective of this routine is to calculate the area
#               of the transition values for different states in GEE.
#               The resulting values are used to create plots.
#
# Author:       Hugo Tameirao Seixas
# Contact:      tameirao.hugo@gmail.com
# Date:         2020-11-10
#
# Notes:        In order to run this routine, you will need to have access
#               to Google Earth Engine (https://earthengine.google.com/).
#               You will also need to install and configure the {rgee} package,
#               Installation and use guide can be found in:
#               https://r-spatial.github.io/rgee/
#
#               Check the "00_config.R" file to set your email to access GEE.
#
# LIBRARIES -------------------------------------------------------------------
#
library(rgee)
library(sf)
library(geobr)
library(lubridate)
library(tidyverse)
#
# OPTIONS ---------------------------------------------------------------------
#
source('00_config.R')
#
# START GEE API ---------------------------------------------------------------

## Initialize GEE ----
ee_Initialize(email = gee_email)

# LOAD DATA -------------------------------------------------------------------

## Get the polygons of Brazilian states ----
states <-
  read_state(year = "2019") %>%
  st_join( # Only ones that overlaps with the Amazon biome
    read_biomes(year = "2019") %>%
      filter(code_biome == 1),
    join = st_overlaps # It's ok that coords are not planar in this case
    # The behavior of this function will change soon in {sf} 1.0
  ) %>%
  drop_na() %>%
  select(code_state, name_state, geom)

## List transition assets on GEE ----
asset_list <-
  ee_manage_assetlist(
    path_asset = "users/hugoseixas/amazon_forest_transition"
    ) %>%
  as_tibble() %>%
  mutate(cycle = str_extract(ID, pattern = "[0-9]+"))

## Get only the first agriculture transition cycle ----
asset <- asset_list %>% filter(cycle == 1) %>% pull(ID)

## Load asset as Image on GEE ----
trans_length <-
  ee$Image(asset)$
  select(
    list("b1", "b2", "b3", "b4", "b5", "b6"),
    list(
      "agri_code", "forest_year", "agri_year",
      "trans_length", "trans_cycle", "forest_type"
    )
  )

## List band names ----
bands_list <- trans_length$bandNames()$getInfo()

## Plot transition length mosaic ----
Map$centerObject(trans_length$geometry())
Map$addLayer(
  eeObject = trans_length,
  visParams = list(
    bands = "trans_length",
    min = 1,
    max = 33,
    palette = viridis::plasma(n = 5)
    # blue = short transition length / yellow = longer transition length
  )
)

# CALCULATE AREAS -------------------------------------------------------------

## Calculate area for each value, of each band, in each state ----
area_collection <-
  map_df(
    .x = states$name_state,
    function(state) {

      cat("\n", state, "\n")

      ## Load one state into GEE ----
      ee_state <-
        sf_as_ee(
          states %>% filter(name_state == state)
        )

      ## Get area for each band in the state -----
      return(
        map_df(
          .x = bands_list,
          function(band) {

            cat(band, " ", sep = " ")

            ## Calculate area by group (discrete values of each band) ----
            areas <-
              ee$Image$
              pixelArea()$
              addBands(trans_length$select(band))$
              reduceRegion(
                reducer = ee$Reducer$sum()$group(
                  groupField = 1L,
                  groupName = "value"
                ),
                geometry = ee_state$first()$geometry(),
                scale = 30,
                maxPixels = 1e13
              )$
              get("groups")$
              getInfo()

            ## Return values as a table ----
            return(
              tibble(key = map(areas, "value"), area = map(areas, "sum")) %>%
                unnest(cols = c(key, area)) %>%
                mutate(state = state, layer = band)
            )

          }
        )
      )

    }
  )

# CREATE PLOTS ----

## Area of deforestation and agriculture establishment years ----
area_collection %>%
  mutate(
    area = area / 1e6, # Convert area to square kilometers
    layer = fct_rev(factor(layer))
    ) %>%
  filter(
    layer %in% c("forest_year", "agri_year"),
    # Filter states with bigger area of transition
    state %in% c("Maranhão", "Mato Grosso", "Pará", "Rondônia")
    ) %>%
  mutate(key = ymd((key + 1984), truncated = 2L)) %>% # Convert years to date
  ggplot() +
  facet_grid(
    layer ~ state,
    scales = "free_y",
    labeller = labeller(
      layer = c(
        "forest_year" = "Deforestation",
        "agri_year" = "Agriculture establishment"
      )
    )
  ) +
  geom_col(aes(x = key, y = area), fill = "#BBBBBB") +
  scale_x_date(
    date_breaks = "6 years",
    date_labels = "%Y",
    expand = c(0.02, 0.02)
  ) +
  labs(y = "Area (km²)", x = "Year") +
  theme_dark() +
  theme(
    text = element_text(size = 11),
    axis.text.y = element_text(angle = 45, vjust = 0.6, hjust = 0.5),
    axis.text.x = element_text(angle = 45, vjust = 0.6, hjust = 0.4)
  ) +
  ggsave(
    "./plots/trans_years_states_area.pdf",
    width = 15,
    height = 12,
    units = "cm"
  )
