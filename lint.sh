poetry run black . --check --color
poetry run isort .
poetry run mypy --config-file=pyproject.toml
