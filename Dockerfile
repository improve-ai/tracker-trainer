FROM python:3.7

WORKDIR /code

# Requirements
COPY ./requirements.txt .
RUN python -m pip install -r requirements.txt

# Code
COPY src/ ./src

COPY tests/ ./tests

ENV PATH_TOWARDS_EFS=$efs_mount_path

ARG test

RUN ls

# https://stackoverflow.com/questions/43654656/dockerfile-if-else-condition-with-external-arguments
# docker build -t my_docker .  --build-arg arg=45
RUN if [ "$test" = "true" ]; then \
        python3 -m pytest -v -s ; \
    else \
        python3 worker.py ; \
    fi

CMD [ "python", "./worker.py" ] 
