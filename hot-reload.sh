#!/bin/sh

watchmedo auto-restart --recursive --patterns="*.py" -- poetry run simple
