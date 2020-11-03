FROM python:3.7

WORKDIR /code

COPY ./requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Code
COPY src/ ./src

# Tests
COPY tests/ ./tests

ENV PYTHONPATH=/code

CMD [ "python", "src/worker.py" ] 
