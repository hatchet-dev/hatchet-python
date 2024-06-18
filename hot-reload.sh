#!/bin/sh

watchmedo auto-restart --recursive --patterns="*.py" -- poetry run python3 ./examples/simple/worker.py
