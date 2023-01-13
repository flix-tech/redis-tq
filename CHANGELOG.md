# Changelog

## 0.0.10 - 2023-01-16

## Fix
* correctly handle race condition causing a completed task to be marked completed twice. Thanks [Avinash](https://github.com/nash0740)

## 0.0.9 - 2023-01-06

## Changes

* Remove deprecated support for Python 3.6, add Python 3.10 and 3.11
## Fix
* avoid having dangling tasks if the client disconnects while starting a new one. Thanks [Avinash](https://github.com/nash0740)

## 0.0.8 - 2021-05-07

* make class import easier, and document it
* check if `lease_timeout` is an actual number when we add a task, to avoid
  issues when we calculate the deadline later
