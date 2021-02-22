# Convert a SpatRaster to a data frame

# Adapted from the tabularaster package create by Michael Sumner.
# Instead of creating a tibble from a raster from {raster} package
# I introduced the raster object from the {terra} package, which the extraction
# of values is faster.

terra_as_tibble <-
  function(
    x,
    cell = TRUE,
    dim = nlyr(x) > 1L,
    value = TRUE,
    xy = FALSE,
    ...
  ) {

    dimindex <- seq(terra::nlyr(x))

    cellvalue <- cellindex <- NULL

    if (value) { cellvalue <- as.vector(terra::values(x)) }

    cellindex <-  rep(seq(terra::ncell(x)), terra::nlyr(x))

    d <- dplyr::bind_cols(cellvalue = cellvalue, cellindex = cellindex)

    if (dim) {

      dimindex <- rep(dimindex, each = terra::ncell(x))

      d[["dimindex"]] <- dimindex

    }

    if (xy) {

      xy <- terra::xyFromCell(x, seq_len(terra::ncell(x)))

      colnames(xy) <- c("x", "y")

      if (terra::nlyr(x) > 1) {

        xy <- xy[rep(seq_len(terra::ncell(x)), terra::nlyr(x)), ]

      }

      d <- dplyr::bind_cols(d, tibble::as_tibble(xy))

    }

    if (!cell) d[["cellindex"]] <- NULL

    return(d)

  }

