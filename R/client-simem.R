#' SIMEM API Client
#'
#' R6 class for accessing SIMEM API data
#'
#' @description
#' This class provides methods to connect to and retrieve data from the SIMEM
#' API, which provides Colombian energy market data with different granularities.
#' It inherits from BaseClient to provide a unified interface.
#'
#' @export
SIMEMClient <- R6::R6Class(
  "SIMEMClient",
  inherit = BaseClient,
  public = list(
    #' @field base_url Base URL for the SIMEM API
    base_url = NULL,
    #' @field available_variables Available variables from SIMEM
    available_variables = NULL,

    #' @description
    #' Initialize the SIMEM client
    #'
    #' @return A new SIMEMClient object
    initialize = function() {
      self$client_type <- "simem"
      self$base_url <- "https://www.simem.co/backend-files/api/PublicData"

      cli::cli_alert_info("Connecting to SIMEM API...")

      # Load available variables
      tryCatch({
        self$available_variables <- private$load_available_variables()
        cli::cli_alert_success(
          "SIMEM client initialized with {nrow(self$available_variables)} variables available"
        )
      }, error = function(e) {
        cli::cli_alert_warning("Could not load variable list: {e$message}")
        self$available_variables <- data.frame(
          CodigoVariable = character(),
          Nombre = character(),
          stringsAsFactors = FALSE
        )
      })
    },

    #' @description
    #' Get available collections (variables/datasets)
    #'
    #' @param collection Optional variable code to filter results
    #' @return Data frame with variable information
    get_collections = function(collection = NULL) {
      if (is.null(collection)) {
        return(self$available_variables)
      } else {
        if (collection %in% self$available_variables$CodigoVariable) {
          return(self$available_variables[
            self$available_variables$CodigoVariable == collection,
          ])
        } else {
          cli::cli_alert_danger("Variable {collection} does not exist")
          return(data.frame())
        }
      }
    },

    #' @description
    #' Request data from the SIMEM API (internal method)
    #'
    #' @param dataset_id Dataset ID
    #' @param start_date Start date in YYYY-MM-DD format
    #' @param end_date End date in YYYY-MM-DD format
    #' @param filter_column Optional column name for filtering
    #' @param filter_values Optional values for filtering
    #' @param use_filter Whether to apply filters
    #' @return Data frame with requested data
    request_data = function(dataset_id,
                           start_date,
                           end_date,
                           filter_column = NULL,
                           filter_values = NULL,
                           use_filter = FALSE) {

      # Create internal SIMEM data retriever
      retriever <- private$SIMEMDataRetriever$new(
        base_url = self$base_url,
        dataset_id = dataset_id,
        start_date = start_date,
        end_date = end_date,
        filter_column = filter_column,
        filter_values = filter_values
      )

      # Get data
      data <- retriever$get_data(use_filter = use_filter)

      return(data)
    },

    #' @description
    #' Get dataset metadata (internal method)
    #'
    #' @param dataset_id Dataset ID
    #' @return List with dataset metadata
    get_dataset_metadata = function(dataset_id) {
      # Create a temporary retriever to get metadata
      retriever <- private$SIMEMDataRetriever$new(
        base_url = self$base_url,
        dataset_id = dataset_id,
        start_date = "1990-01-01",
        end_date = Sys.Date()
      )

      metadata <- list(
        name = retriever$get_name(),
        granularity = retriever$get_granularity(),
        columns = retriever$get_columns(),
        dataset_info = retriever$get_dataset_info()
      )

      return(metadata)
    }
  ),

  private = list(
    #' Load available variables from SIMEM
    load_available_variables = function() {
      url <- "https://www.simem.co/backend-datos/vars/listado_variables.json"

      response <- httr2::request(url) |>
        httr2::req_perform()

      if (httr2::resp_is_error(response)) {
        stop("Failed to fetch variable list")
      }

      # Parse JSON regardless of content-type header
      # SIMEM returns text/plain instead of application/json
      json_data <- httr2::resp_body_json(response, check_type = FALSE)

      # Extract all variable information preserving structure
      variables_list <- json_data$variable
      variable_codes <- names(variables_list)

      # Get all unique field names across all variables
      all_fields <- unique(unlist(lapply(variables_list, names)))

      # Create a list to store each column
      df_columns <- list(
        CodigoVariable = variable_codes
      )

      # Extract each field for all variables
      for (field in all_fields) {
        df_columns[[field]] <- lapply(variables_list, function(var) {
          val <- var[[field]]
          # Return the value as-is (preserves NULL, lists, scalars)
          if (is.null(val)) {
            return(NULL)
          } else {
            return(val)
          }
        })
      }

      # Convert to tibble (handles list columns properly)
      variables_df <- tibble::as_tibble(df_columns)

      # Unnest all columns to flatten the structure
      variables_df <- variables_df |>
        tidyr::unnest(cols = dplyr::everything())

      return(variables_df)
    },

    #' Internal class for SIMEM data retrieval
    #' This encapsulates the previous SIMEMClient functionality
    SIMEMDataRetriever = R6::R6Class(
      "SIMEMDataRetriever",
      public = list(
        base_url = NULL,
        dataset_id = NULL,
        start_date = NULL,
        end_date = NULL,
        filter_column = NULL,
        filter_values = NULL,
        dataset_info = NULL,

        initialize = function(base_url, dataset_id, start_date, end_date,
                            filter_column = NULL, filter_values = NULL) {
          self$base_url <- base_url
          self$dataset_id <- private$validate_dataset_id(dataset_id)
          self$start_date <- as.Date(start_date)
          self$end_date <- as.Date(end_date)
          self$filter_column <- filter_column
          self$filter_values <- filter_values

          # Initialize dataset information
          private$set_dataset_data()
        },

        get_name = function() {
          if (is.null(self$dataset_info)) return("")
          return(self$dataset_info$name)
        },

        get_granularity = function() {
          if (is.null(self$dataset_info)) return("")
          return(self$dataset_info$granularity)
        },

        get_columns = function() {
          if (is.null(self$dataset_info)) return(data.frame())
          return(self$dataset_info$columns)
        },

        get_dataset_info = function() {
          return(self$dataset_info)
        },

        get_data = function(use_filter = FALSE) {
          # Check if this is a catalog dataset
          is_catalog <- !is.null(self$dataset_info$is_catalog) &&
            self$dataset_info$is_catalog

          if (is_catalog) {
            urls <- list(private$create_catalog_url(use_filter))
          } else {
            uses_date_filter <- !is.null(self$dataset_info$filter_date)

            if (uses_date_filter) {
              resolution <- private$get_resolution()
              urls <- private$create_urls(self$start_date, self$end_date, resolution, use_filter)
            } else {
              urls <- list(private$create_catalog_url(use_filter))
            }
          }

          # Make requests sequentially
          all_records <- list()

          for (i in seq_along(urls)) {
            tryCatch({
              records <- private$get_records(urls[[i]])
              if (length(records) > 0) {
                all_records[[i]] <- records
              }
            }, error = function(e) {
              cli::cli_alert_warning("Error in request {i}: {e$message}")
            })

            if (i < length(urls) && length(urls) > 1) {
              Sys.sleep(0.1)
            }
          }

          # Combine all records
          if (length(all_records) > 0) {
            flat_records <- unlist(all_records, recursive = FALSE)

            if (length(flat_records) > 0) {
              result <- private$process_records(flat_records)
            } else {
              result <- data.frame()
            }
          } else {
            result <- data.frame()
          }

          return(result)
        }
      ),

      private = list(
        validate_dataset_id = function(dataset_id) {
          if (!is.character(dataset_id) || length(dataset_id) != 1) {
            stop("Dataset ID must be a single character string")
          }

          dataset_id <- trimws(dataset_id)

          if (nchar(dataset_id) > 6 || nchar(dataset_id) == 0) {
            stop("Invalid dataset ID length")
          }

          if (!grepl("^[a-zA-Z0-9]+$", dataset_id)) {
            stop("Dataset ID must be alphanumeric")
          }

          return(tolower(dataset_id))
        },

        set_dataset_data = function() {
          is_catalog <- self$dataset_id %in% c("e007fb", "a5a6c4")

          if (is_catalog) {
            url <- paste0(self$base_url, "?datasetId=", self$dataset_id)
          } else {
            reference_date <- "1990-01-01"
            url <- paste0(
              self$base_url,
              "?startDate=", reference_date,
              "&endDate=", reference_date,
              "&datasetId=", self$dataset_id
            )
          }

          response <- private$make_request(url)

          if (is.null(response) || !response$success) {
            if (!is_catalog) {
              url <- paste0(self$base_url, "?datasetId=", self$dataset_id)
              response <- private$make_request(url)
            }

            if (is.null(response) || !response$success) {
              stop("Failed to retrieve dataset metadata")
            }
          }

          result <- response$result
          metadata <- result$metadata

          self$dataset_info <- list(
            name = result$name,
            granularity = if (!is.null(metadata)) metadata$granularity else "Unknown",
            columns = if (!is.null(result$columns)) as.data.frame(result$columns) else data.frame(),
            filter_date = result$filterDate,
            metadata = metadata,
            is_catalog = is_catalog
          )
        },

        make_request = function(url) {
          tryCatch({
            response <- httr2::request(url) |>
              httr2::req_perform()

            if (httr2::resp_is_error(response)) {
              return(NULL)
            }

            # Parse JSON regardless of content-type header
            # SIMEM may return text/plain instead of application/json
            data <- httr2::resp_body_json(response, check_type = FALSE)
            return(data)

          }, error = function(e) {
            return(NULL)
          })
        },

        get_resolution = function() {
          granularity <- self$dataset_info$granularity

          if (is.null(granularity)) return(1)

          if (granularity %in% c("Diaria", "Horaria")) {
            return(1)
          } else if (granularity %in% c("Mensual", "Semanal")) {
            return(24)
          } else if (granularity == "Anual") {
            return(60)
          } else {
            return(1)
          }
        },

        generate_dates = function(start_date, end_date, resolution) {
          start_dates <- seq(
            from = lubridate::floor_date(start_date, "month"),
            to = end_date,
            by = "month"
          )

          if (start_date > start_dates[1]) {
            start_dates[1] <- start_date
          }

          if (resolution > 1) {
            indices <- seq(1, length(start_dates), by = resolution)
            start_dates <- start_dates[indices]
          }

          end_dates <- c(start_dates[-1] - lubridate::days(1), end_date)

          return(list(starts = start_dates, ends = end_dates))
        },

        create_urls = function(start_date, end_date, resolution, use_filter = FALSE) {
          date_ranges <- private$generate_dates(start_date, end_date, resolution)

          urls <- mapply(function(start, end) {
            url <- paste0(
              self$base_url,
              "?startDate=", as.character(start),
              "&endDate=", as.character(end),
              "&datasetId=", self$dataset_id
            )

            if (use_filter && !is.null(self$filter_column) && !is.null(self$filter_values)) {
              filter_params <- paste0(
                "&columnDestinyName=", self$filter_column,
                "&values=", paste(self$filter_values, collapse = ",")
              )
              url <- paste0(url, filter_params)
            }

            return(url)
          }, date_ranges$starts, date_ranges$ends, SIMPLIFY = FALSE)

          return(urls)
        },

        create_catalog_url = function(use_filter = FALSE) {
          url <- paste0(self$base_url, "?datasetId=", self$dataset_id)

          if (use_filter && !is.null(self$filter_column) && !is.null(self$filter_values)) {
            filter_params <- paste0(
              "&columnDestinyName=", self$filter_column,
              "&values=", paste(self$filter_values, collapse = ",")
            )
            url <- paste0(url, filter_params)
          }

          return(url)
        },

        get_records = function(url) {
          response <- private$make_request(url)

          if (is.null(response) || !response$success) {
            return(list())
          }

          result <- response$result

          if (is.null(result$records) || length(result$records) == 0) {
            return(list())
          }

          return(result$records)
        },

        process_records = function(records) {
          records <- lapply(records, function(x) {
            lapply(x, function(y) {
              if (is.null(y)) {
                NA
              } else if (identical(y, "<null>")) {
                NA
              } else {
                y
              }
            })
          })

          df_chunk <- jsonlite::fromJSON(jsonlite::toJSON(records), flatten = TRUE)

          list_cols <- sapply(df_chunk, is.list)
          if (any(list_cols)) {
            df_chunk[list_cols] <- lapply(df_chunk[list_cols], unlist)
          }

          result <- private$process_result(df_chunk)

          return(result)
        },

        process_result = function(df) {
          return(df)
        }
      )
    )
  )
)

#' @title SIMEM Variable Client
#' @description R6 class for accessing specific SIMEM variables
#' @export
SIMEMVariable <- R6::R6Class(
  "SIMEMVariable",
  public = list(
    #' @field cod_variable Variable code
    cod_variable = NULL,
    #' @field start_date Start date for data retrieval
    start_date = NULL,
    #' @field end_date End date for data retrieval
    end_date = NULL,
    #' @field version Version of the variable
    version = NULL,
    #' @field variable_info Variable configuration information
    variable_info = NULL,

    #' @description
    #' Initialize the SIMEM variable client
    #'
    #' @param cod_variable Character string with variable code
    #' @param start_date Start date in YYYY-MM-DD format
    #' @param end_date End date in YYYY-MM-DD format
    #' @param version Version of the variable (default: 0)
    #' @return A new SIMEMVariable object
    initialize = function(cod_variable,
                          start_date,
                          end_date,
                          version = 0) {
      # Load variable configuration
      json_config <- private$read_json_config()

      if (!cod_variable %in% names(json_config$variable)) {
        stop(paste("Variable", cod_variable, "is not available"))
      }

      self$cod_variable <- cod_variable
      self$start_date <- as.Date(start_date)
      self$end_date <- as.Date(end_date)
      self$version <- version

      # Store variable configuration
      self$variable_info <- json_config$variable[[cod_variable]]

      cli::cli_alert_success("SIMEM variable client initialized for: {self$variable_info$name}")
    },

    #' @description
    #' Get variable data
    #'
    #' @return Data frame with variable data
    get_data = function() {
      # Create SIMEM client
      simem_client <- SIMEMClient$new()

      # Get data using the client's request_data method
      data <- simem_client$request_data(
        dataset_id = self$variable_info$dataset_id,
        start_date = self$start_date,
        end_date = self$end_date,
        filter_column = self$variable_info$var_column,
        filter_values = self$cod_variable,
        use_filter = !is.null(self$variable_info$var_column)
      )

      if (nrow(data) == 0) {
        cli::cli_alert_warning("No data found for variable {self$cod_variable}")
        return(data.frame())
      }

      # Process data based on variable configuration
      processed_data <- private$process_variable_data(data)

      return(processed_data)
    },

    #' @description
    #' Get variable statistics
    #'
    #' @return List with variable statistics
    get_stats = function() {
      data <- self$get_data()

      if (nrow(data) == 0) {
        return(list())
      }

      value_col <- self$variable_info$value_column

      if (is.null(value_col) || !value_col %in% names(data)) {
        cli::cli_alert_warning("Value column not found in data")
        return(list())
      }

      values <- as.numeric(data[[value_col]])
      values <- values[!is.na(values)]

      if (length(values) == 0) {
        return(list())
      }

      stats <- list(
        mean = mean(values, na.rm = TRUE),
        median = median(values, na.rm = TRUE),
        std_dev = sd(values, na.rm = TRUE),
        min = min(values, na.rm = TRUE),
        max = max(values, na.rm = TRUE),
        null_count = sum(is.na(as.numeric(data[[value_col]]))),
        zero_count = sum(values == 0, na.rm = TRUE),
        start_date = as.character(self$start_date),
        end_date = as.character(self$end_date),
        variable_name = self$variable_info$name
      )

      return(stats)
    }
  ),

  private = list(
    #' Read JSON configuration for variables
    read_json_config = function() {
      url <- "https://www.simem.co/backend-datos/vars/listado_variables.json"

      tryCatch({
        response <- httr2::request(url) |>
          httr2::req_perform()

        if (httr2::resp_is_error(response)) {
          stop("Failed to fetch variable configuration")
        }

        # Parse JSON regardless of content-type header
        # SIMEM returns text/plain instead of application/json
        json_data <- httr2::resp_body_json(response, check_type = FALSE)
        return(json_data)

      }, error = function(e) {
        stop(paste("Error reading variable configuration:", e$message))
      })
    },

    #' Process variable data based on configuration
    process_variable_data = function(data) {
      if (nrow(data) == 0) {
        return(data)
      }

      # Rename value column if specified
      value_col <- self$variable_info$value_column
      if (!is.null(value_col) && value_col %in% names(data)) {
        data[[value_col]] <- as.numeric(data[[value_col]])
      }

      # Process date columns
      date_col <- self$variable_info$date_column
      if (!is.null(date_col) && date_col %in% names(data)) {
        data[[date_col]] <- as.Date(data[[date_col]])
      }

      return(data)
    }
  )
)

#' @title Get SIMEM Variable Collection
#' @description Get list of available SIMEM variables with all metadata
#' @return Tibble with available variables and all their metadata fields.
#'   List columns are preserved for fields like dimensions.
#' @export
get_simem_variables <- function() {
  url <- "https://www.simem.co/backend-datos/vars/listado_variables.json"

  tryCatch({
    response <- httr2::request(url) |>
      httr2::req_perform()

    if (httr2::resp_is_error(response)) {
      stop("Failed to fetch variable list")
    }

    # Parse JSON regardless of content-type header
    # SIMEM returns text/plain instead of application/json
    json_data <- httr2::resp_body_json(response, check_type = FALSE)

    # Extract all variable information preserving structure
    variables_list <- json_data$variable
    variable_codes <- names(variables_list)

    # Get all unique field names across all variables
    all_fields <- unique(unlist(lapply(variables_list, names)))

    # Create a list to store each column
    df_columns <- list(
      CodigoVariable = variable_codes
    )

    # Extract each field for all variables
    for (field in all_fields) {
      df_columns[[field]] <- lapply(variables_list, function(var) {
        val <- var[[field]]
        # Return the value as-is (preserves NULL, lists, scalars)
        if (is.null(val)) {
          return(NULL)
        } else {
          return(val)
        }
      })
    }

    # Convert to tibble (handles list columns properly)
    variables_df <- tibble::as_tibble(df_columns)

    # Unnest all columns to flatten the structure
    variables_df <- variables_df |>
      tidyr::unnest(cols = dplyr::everything())

    return(variables_df)

  }, error = function(e) {
    cli::cli_alert_danger("Error fetching variables: {e$message}")
    return(tibble::tibble())
  })
}

#' @title SIMEM Catalog Client
#' @description R6 class for accessing SIMEM catalog information
#' @export
SIMEMCatalog <- R6::R6Class(
  "SIMEMCatalog",
  public = list(
    #' @field catalog_type Type of catalog (datasets or variables)
    catalog_type = NULL,
    #' @field data Catalog data
    data = NULL,

    #' @description
    #' Initialize the SIMEM catalog client
    #'
    #' @param catalog_type Type of catalog: "datasets" or "variables"
    #' @return A new SIMEMCatalog object
    initialize = function(catalog_type) {
      catalog_type <- tolower(catalog_type)

      if (!catalog_type %in% c("datasets", "variables")) {
        stop("Catalog type must be 'datasets' or 'variables'")
      }

      self$catalog_type <- catalog_type

      # Set dataset ID based on catalog type
      if (catalog_type == "datasets") {
        dataset_id <- "e007fb"
      } else {
        dataset_id <- "a5a6c4"
      }

      # Get catalog data using the SIMEM client
      simem_client <- SIMEMClient$new()

      self$data <- simem_client$request_data(
        dataset_id = dataset_id,
        start_date = "1990-01-01",
        end_date = Sys.Date(),
        use_filter = FALSE
      )

      cli::cli_alert_success("SIMEM catalog loaded: {nrow(self$data)} {catalog_type} found")
    },

    #' @description
    #' Get catalog data
    #'
    #' @return Data frame with catalog information
    get_data = function() {
      return(self$data)
    }
  )
)
