FROM python:3.7

WORKDIR /code

COPY ./requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Code
COPY src/ ./src

# Tests
COPY tests/ ./tests
COPY ./conftest.py .
COPY ./pytest.ini .

ENV PYTHONPATH=/code

RUN python src/create_test_env.py

CMD [ "python", "src/worker.py" ] 
