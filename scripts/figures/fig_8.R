# HEADER ----------------------------------------------------------------------
#
# Title:        Create figure 8
# Description:  The objective of this routine is compare the conversion length
#               results, comparing them with visual observations.
#
#
# Author:       Hugo Tameirao Seixas
# Contact:      seixas.hugo@protonmail.com
# Date:         2022-08-03
#
# Notes:
#
#
#
#
# LIBRARIES -------------------------------------------------------------------
#
library(arrow)
library(magrittr)
library(lubridate)
library(glue)
library(dplyr)
library(units)
library(tibble)
library(readr)
library(tidyr)
library(forcats)
library(purrr)
library(scales)
library(ggplot2)
library(ggtext)
#
# OPTIONS ---------------------------------------------------------------------
#
#
# LOAD DATA -------------------------------------------------------------------

## Load observed data ----
observed <- read_csv("data/validation/results.csv") %>%
  mutate( # Remove points where there were no conversion
    forest_year = if_else(forest_year <= 0, NA_real_, forest_year),
    agri_year = if_else(agri_year <= 0, NA_real_, agri_year),
    c_length = if_else(agri_year <= 0, NA_real_, c_length),
    c_length = if_else(forest_year <= 0, NA_real_, c_length)
  ) %>%
  mutate(across(forest_year:agri_year, ~ .x - 1984)) %>%
  pivot_longer(
    cols =  c(forest_year, agri_year, c_length),
    names_to = "var",
    values_to = "observed"
  ) %>%
  arrange(cell_id)

## Load estimates data ----
estimated <- open_dataset("data/c_tabular_dataset/c_length/")

estimated %<>%
  filter(
    cell_id %in% observed$cell_id
  ) %>%
  collect() %>%
  mutate(across(forest_year:agri_year, ~ .x - 1984)) %>%
  pivot_longer(
    cols =  c(forest_year, agri_year, c_length),
    names_to = "var",
    values_to = "estimated"
  ) %>%
  arrange(cell_id)

# Check if there is any difference between tables
setdiff(unique(observed$cell_id), unique(estimated$cell_id))

## Join tables ----
comparison <- estimated %>%
  inner_join(observed, by = c("cell_id", "var")) %>%
  mutate(
    difference = observed - estimated,
    var = factor(
      var,
      levels = c("forest_year", "agri_year", "c_length"),
      labels = c(
        "Deforestation", "Agriculture Establishment", "Conversion Length"
      )
    )
  )

## Calculate quantiles of errors
quants <- comparison %>%
  group_by(var) %>%
  summarise(
    value = quantile(
      difference,
      probs = c(0.05, 0.25, 0.75, 0.95),
      na.rm = TRUE
    )
  ) %>%
  mutate(
    lim = c("min", "min", "max", "max"),
    group_id = c(1, 2, 2, 1)
  ) %>%
  pivot_wider(
    id_cols = c(var, group_id),
    names_from = lim,
    values_from = value
  )

quants <- do.call("rbind", replicate(38, quants, simplify = FALSE)) %>%
  group_by(var, group_id) %>%
  mutate(estimated = -1:36, observed = -1:36)

## Save plots data ----

# Comparison table
write_csv(comparison, "data/figures/fig_4_5.csv")

# Quantiles
write_csv(quants, "data/figures/fig_5_quants.csv")

# ANALYSE RESULTS -------------------------------------------------------------

## Create scatter plot with the errors ----
scatter_plot <- comparison %>%
  ggplot() +
  facet_wrap( ~ var) +
  ggtitle("Dispersion between observed and estimated conversions.") +
  geom_ribbon(
    data = subset(quants, group_id == 2),
    aes(
      ymin = observed + min, ymax = max + observed,
      xmin = estimated + min, xmax = max + estimated,
      x = estimated, y = observed
    ),
    fill = "white", alpha = 0.7
  ) +
  geom_ribbon(
    data = subset(quants, group_id == 1),
    aes(
      ymin = observed + min, ymax = max + observed,
      xmin = estimated + min, xmax = max + estimated,
      x = estimated, y = observed
    ),
    fill = "white", alpha = 0.3
  ) +
  geom_abline(intercept = 0, slope = 1, color = "#6a6a6a") +
  geom_text(
    data = tibble(
      quant = c("5%", "25%", "75%", "95%"),
      observed = c(17 + 12, 17 + 4.75, 17 - 1.75, 17 - 8.7),
      estimated = c(16.5, 16.5, 16.5, 16.5),
      var = factor("Agriculture Establishment")
    ),
    aes(x = estimated, y = observed, label = quant),
    hjust = "left",
    size = 2.3,
    fontface = "bold"
  ) +
  geom_point(
    aes(x = estimated, y = observed),
    fill = "black",
    size = 0.6
  ) +
  geom_text(
    data = tibble(
      label = c("(a)", "(b)", "(c)"),
      var = factor(
        c("Deforestation", "Agriculture Establishment", "Conversion Length")
      ),
      estimated = rep(1, 3),
      observed = rep(35, 3)
    ),
    aes(x = estimated, y = observed, label = label),
    size = 3,
    color = "white"
  ) +
  scale_x_continuous(breaks = pretty_breaks(n = 5), expand = c(0, 0)) +
  scale_y_continuous(breaks = pretty_breaks(n = 5), expand = c(0.01, 0.01)) +
  coord_fixed(
    ylim = c(-1, 36), xlim = c(-1, 36),
    expand = TRUE,
    clip = "on"
  ) +
  labs(x = "Estimated", y = "Observed") +
  theme_dark() +
  theme(
    text = element_text(size = 11),
    axis.text.y = element_text(angle = 45, vjust = 0.6, hjust = 0.5),
    axis.text.x = element_text(angle = 45, vjust = 0.6, hjust = 0.4),
    legend.position = "bottom"
  ) +
  guides(
    fill = guide_colourbar(
      barwidth = 10,
      barheight = 0.8,
      title.vjust = 1
    ),
    color = "none"
  )

ggsave(
  "./figs/error_scatter.png",
  scatter_plot,
  width = 17,
  height = 8,
  units = "cm",
  dpi = 300
)
