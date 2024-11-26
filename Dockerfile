FROM python:3.11-slim-buster

RUN pip install poetry==1.6.1

ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_CREATE=1
ENV POETRY_VIRTUALENVS_IN_PROJECT=1

COPY . .

RUN poetry install


CMD ["poetry", "run", "python", "examples/async/worker.py"]