# Changelog

## [UNRELEASED] - 2021-05-07

* check if `lease_timeout` is an actual number when we add a task, to avoid
  issues when we calculate the deadline later
