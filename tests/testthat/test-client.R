test_that("client() creates XMClient for 'xm'", {
  skip_if_offline()
  xm <- client("xm")
  expect_s3_class(xm, "XMClient")
  expect_equal(xm$client_type, "xm")
})

test_that("client() creates SIMEMClient for 'simem'", {
  skip_if_offline()
  simem <- client("simem")
  expect_s3_class(simem, "SIMEMClient")
  expect_equal(simem$client_type, "simem")
})

test_that("client() rejects invalid type", {
  expect_error(client("invalid"), "Invalid client type")
})

test_that("client() requires type parameter", {
  expect_error(client(), "Client type is required")
})

# Helper function to skip tests when offline
skip_if_offline <- function() {
  tryCatch(
    {
      httr2::request("https://www.google.com") |>
        httr2::req_timeout(3) |>
        httr2::req_perform()
    },
    error = function(e) {
      skip("No internet connection")
    }
  )
}