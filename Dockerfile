FROM python:3.7

WORKDIR /code

COPY ./requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Code
COPY src/ ./src

# Tests
COPY tests/ ./tests

RUN ["python" "-m" "pytest", "-vs"]

ARG test
RUN if [ "$test" = "true" ]; then \
        python -m pytest -v -s ; \
    else \
        python worker.py ; \
    fi

CMD [ "python", "./src/worker.py" ] 
