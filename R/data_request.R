#' Data Request Configuration
#'
#' @description
#' R6 class that encapsulates a data request configuration and provides methods
#' to execute the request and retrieve metadata. This class provides a unified
#' interface for both XM and SIMEM data sources.
#'
#' @details
#' This class should not be instantiated directly. Use the \code{config_request()}
#' method from a client object (XMClient or SIMEMClient) to create data requests.
#'
DataRequest <- R6::R6Class(
  "DataRequest",
  public = list(
    #' @field client Parent client object (XMClient or SIMEMClient)
    client = NULL,
    #' @field collection Collection or dataset ID
    collection = NULL,
    #' @field metric Metric or variable ID
    metric = NULL,
    #' @field start_date Start date for data retrieval
    start_date = NULL,
    #' @field end_date End date for data retrieval
    end_date = NULL,
    #' @field params Named list of additional filter parameters
    params = NULL,

    #' @description
    #' Initialize a new DataRequest object
    #'
    #' @param client Parent client object
    #' @param collection Collection or dataset ID
    #' @param metric Metric or variable ID
    #' @param start_date Start date in YYYY-MM-DD format
    #' @param end_date End date in YYYY-MM-DD format
    #' @param params Optional named list of additional filter parameters
    #' @return A new DataRequest object
    initialize = function(client, collection, metric, start_date, end_date, params = NULL) {
      self$client <- client
      self$collection <- collection
      self$metric <- metric
      self$start_date <- as.Date(start_date)
      self$end_date <- as.Date(end_date)
      self$params <- params

      # Validate the request configuration
      private$validate_request()

      cli::cli_alert_success(
        "Data request configured for {.val {collection}} / {.val {metric}}"
      )
    },

    #' @description
    #' Execute the data request and retrieve data
    #'
    #' @return Data frame with requested data
    get_data = function() {
      cli::cli_alert_info("Retrieving data from {self$client$client_type} API...")

      # Delegate to client-specific implementation
      if (self$client$client_type == "xm") {
        data <- private$get_xm_data()
      } else if (self$client$client_type == "simem") {
        data <- private$get_simem_data()
      } else {
        cli::cli_abort("Unknown client type: {self$client$client_type}")
      }

      if (nrow(data) == 0) {
        cli::cli_alert_warning("No data returned for this request")
      } else {
        cli::cli_alert_success("Retrieved {nrow(data)} rows")
      }

      return(data)
    },

    #' @description
    #' Get metadata about the collection/dataset
    #'
    #' @return List or data frame with metadata information
    get_metadata = function() {
      cli::cli_alert_info("Retrieving metadata from {self$client$client_type} API...")

      # Delegate to client-specific implementation
      if (self$client$client_type == "xm") {
        metadata <- private$get_xm_metadata()
      } else if (self$client$client_type == "simem") {
        metadata <- private$get_simem_metadata()
      } else {
        cli::cli_abort("Unknown client type: {self$client$client_type}")
      }

      return(metadata)
    }
  ),

  private = list(
    #' Validate the request configuration
    validate_request = function() {
      # Get available collections from parent client
      collections <- self$client$get_collections()

      if (nrow(collections) == 0) {
        cli::cli_abort("No collections available from client")
      }

      # Client-specific validation
      if (self$client$client_type == "xm") {
        private$validate_xm_request(collections)
      } else if (self$client$client_type == "simem") {
        private$validate_simem_request(collections)
      }
    },

    #' Validate XM-specific request
    validate_xm_request = function(collections) {
      # Check if collection exists
      if (!self$collection %in% collections$MetricId) {
        cli::cli_abort(
          "Collection {.val {self$collection}} not found in XM API"
        )
      }

      # Check if metric exists
      if (!self$metric %in% collections$Entity) {
        cli::cli_abort(
          "Metric {.val {self$metric}} not found in XM API"
        )
      }

      # Validate that collection and metric combination exists
      valid_combo <- any(
        collections$MetricId == self$collection &
        collections$Entity == self$metric
      )

      if (!valid_combo) {
        cli::cli_abort(
          "Invalid combination: collection {.val {self$collection}} and metric {.val {self$metric}}"
        )
      }
    },

    #' Validate SIMEM-specific request
    validate_simem_request = function(collections) {
      # For SIMEM, collection is dataset_id and metric is variable code
      # Validation is less strict as SIMEM has a different structure
      # The actual validation happens when the SIMEM client is created
      invisible(NULL)
    },

    #' Get data from XM API
    get_xm_data = function() {
      # Convert params to filtros for XM API
      filtros <- self$params

      # Use the XM client's request_data method
      data <- self$client$request_data(
        coleccion = self$collection,
        metrica = self$metric,
        start_date = as.character(self$start_date),
        end_date = as.character(self$end_date),
        filtros = filtros
      )

      return(data)
    },

    #' Get data from SIMEM API
    get_simem_data = function() {
      # Extract filter parameters from params
      filter_column <- NULL
      filter_values <- NULL

      if (!is.null(self$params)) {
        # Use params directly for filtering
        if ("filter_column" %in% names(self$params)) {
          filter_column <- self$params$filter_column
        }
        if ("filter_values" %in% names(self$params)) {
          filter_values <- self$params$filter_values
        }
      }

      # Use the parent client's request_data method
      use_filter <- !is.null(filter_column) && !is.null(filter_values)

      data <- self$client$request_data(
        dataset_id = self$collection,
        start_date = as.character(self$start_date),
        end_date = as.character(self$end_date),
        filter_column = filter_column,
        filter_values = filter_values,
        use_filter = use_filter
      )

      return(data)
    },

    #' Get metadata from XM API
    get_xm_metadata = function() {
      # Get collection information
      collection_info <- self$client$get_collections(coleccion = self$collection)

      # Filter to specific metric
      metadata <- collection_info[collection_info$Entity == self$metric, ]

      return(metadata)
    },

    #' Get metadata from SIMEM API
    get_simem_metadata = function() {
      # Use the parent client's get_dataset_metadata method
      metadata <- self$client$get_dataset_metadata(
        dataset_id = self$collection
      )

      return(metadata)
    }
  )
)
