
library(sf)
library(terra)
library(terrainr)
library(magrittr)
library(fs)
library(lubridate)
library(glue)
library(dplyr)
library(tibble)
library(readr)
library(tidyr)
library(forcats)
library(purrr)
library(stringr)
library(ggplot2)
library(shiny)
library(shinythemes)
library(gridlayout)

proj_path <- here::here()

sample_paths <- dir_ls(glue("{proj_path}/data/validation/landsat/"))

sample_cell <- as.list(as.integer(str_extract(sample_paths, "(\\d)+")))
names(sample_cell) <- as.character(1:length(sample_cell))

sample_points <- read_sf(glue("{proj_path}/data/validation/sample.fgb"))

results <-
  tibble(
    sample_id = NA_real_,
    cell_id = NA_real_,
    forest_year = NA_real_,
    agri_year = NA_real_,
    trans_length = NA_real_
  ) %>%
  drop_na()

if (!file_exists(glue("{proj_path}/data/validation/results.csv"))) {

  write_csv(
    results,
    glue("{proj_path}/data/validation/results.csv")
  )

}

# Define app template
ui <- grid_page(
  theme = bslib::bs_theme(bootswatch = "slate"),
  layout = c(
    "header  header      header       header     ",
    "sidebar composite1  composite2   composite3 ",
    "sidebar forest      agri         length     "
  ),
  row_sizes = c(
    "70px",
    "1fr",
    "1fr"
  ),
  col_sizes = c(
    "225px",
    "1fr",
    "1fr",
    "1fr"
  ),
  gap_size = "1rem",
  grid_card(
    area = "sidebar",
    item_alignment = "top",
    title = "Settings",
    item_gap = "12px",
    selectInput(
      inputId = "sampleCell",
      label = "Sample",
      choices = sample_cell
    ),
    radioButtons(
      inputId = "seasonOpt",
      label = "Season",
      choices = list(
        Wet = "wet",
        Dry = "dry"
      ),
      width = "100%"
    ),
    numericInput(
      inputId = "imageYear",
      label = "View Year",
      value = 1985L,
      min = 1985L,
      max = 2020L,
      step = 1L,
      width = "auto"
    ),
    numericInput(
      inputId = "transBegin",
      label = "Forest to Pasture",
      value = 1985L,
      min = 1985L,
      max = 2020L,
      step = 1L,
      width = "auto"
    ),
    numericInput(
      inputId = "transEnd",
      label = "Pasture to Agriculture",
      value = 1985L,
      min = 1985L,
      max = 2020L,
      step = 1L,
      width = "auto"
    ),
    actionButton(
      inputId = "saveResults",
      label = "Save "
    )
  ),
  grid_card_text(
    area = "header",
    content = "Forest to agriculture transition: accuracy assessment",
    alignment = "center",
    is_title = FALSE
  ),
  grid_card_plot(area = "composite1"),
  grid_card_plot(area = "composite2"),
  grid_card_plot(area = "composite3"),
  grid_card_plot(area = "forest"),
  grid_card_plot(area = "agri"),
  grid_card_plot(area = "lenght")
)

# Define server logic
server <-
  function(input, output) {

    output$composite1 <- renderPlot({

      img <-
        rast(
          dir_ls(
            glue(
              "{proj_path}/data/validation/landsat/{input$sampleCell}/"
            ),
            regexp = input$imageYear
          )
        )

      img[is.na(img)] <- 0

      minmax = range(values(img), na.rm = TRUE)

      img <- (img - minmax[1]) / (diff(minmax))

      img_df <- as.data.frame(img, xy = TRUE)

      if (input$seasonOpt == "dry") {

        img_df <- img_df %>%
          select(
            x, y,
            red = red_median_dry,
            green = green_median_dry,
            blue = blue_median_dry
          )

      } else {

        img_df <- img_df %>%
          select(
            x, y,
            red = red_median_wet,
            green = green_median_wet,
            blue = blue_median_wet
          )

      }

      point <- sample_points %>%
        filter(cell_id == input$sampleCell)

      ggplot() +
        geom_spatial_rgb(
          data = img_df,
          aes(
            x = x, y = y,
            r = red, g = green, b = blue
          )
        ) +
        geom_sf(
          data = point,
          alpha = 0.5
        ) +
        theme_void() +
        labs(title = "RED-GREEN-BLUE") +
        theme(plot.title = element_text(size = 14, face = "bold"))

    })

    output$composite2 <- renderPlot({

      img <-
        rast(
          dir_ls(
            glue(
              "{proj_path}/data/validation/landsat/{input$sampleCell}/"
            ),
            regexp = input$imageYear
          )
        )

      img[is.na(img)] <- 0

      minmax = range(values(img), na.rm = TRUE)

      img <- (img - minmax[1]) / (diff(minmax))

      img_df <- as.data.frame(img, xy = TRUE)

      if (input$seasonOpt == "dry") {

        img_df <- img_df %>%
          select(
            x, y,
            red = swir1_median_dry,
            green = nir_median_dry,
            blue = blue_median_dry
          )

      } else {

        img_df <- img_df %>%
          select(
            x, y,
            red = swir1_median_wet,
            green = nir_median_wet,
            blue = blue_median_wet
          )

      }

      point <- sample_points %>%
        filter(cell_id == input$sampleCell)

      ggplot() +
        geom_spatial_rgb(
          data = img_df,
          aes(
            x = x, y = y,
            r = red, g = green, b = blue
          )
        ) +
        geom_sf(
          data = point,
          alpha = 0.5
        ) +
        theme_void() +
        labs(title = "NIR-GREEN-BLUE") +
        theme(plot.title = element_text(size = 14, face = "bold"))

    })

    output$composite3 <- renderPlot({

      img <-
        rast(
          dir_ls(
            glue(
              "{proj_path}/data/validation/landsat/{input$sampleCell}/"
            ),
            regexp = input$imageYear
          )
        )

      img[is.na(img)] <- 0

      img_df <- as.data.frame(img, xy = TRUE)

      point <- sample_points %>%
        filter(cell_id == input$sampleCell)

      ggplot() +
        geom_raster(
          data = img_df,
          aes(x = x, y = y, fill = ndvi_amp)
        ) +
        geom_sf(
          data = point,
          alpha = 0.5
        ) +
        scale_fill_viridis_c() +
        theme_void() +
        labs(title = "NDVI Amplitude") +
        theme(
          plot.title = element_text(size = 14, face = "bold"),
          legend.position = ""
        )

    })

    resultsTable <- reactive({
      new_results <- results %>%
        add_row(
          sample_id = as.double(filter(stack(sample_cell), values == input$sampleCell)[[2]]),
          cell_id = as.double(input$sampleCell),
          forest_year = input$transBegin,
          agri_year = input$transEnd,
          trans_length = input$transEnd - input$transBegin
        )
      new_results
    })

    observeEvent(
      input$saveResults,
      {
        write_csv(
          resultsTable(),
          glue("{proj_path}/data/validation/results.csv"),
          append = TRUE
        )
      }
    )

  }

shinyApp(ui, server)
