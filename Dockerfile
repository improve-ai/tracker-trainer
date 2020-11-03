FROM python:3.7

WORKDIR /code

# Requirements
COPY ./requirements.txt .
RUN python -m pip install -r requirements.txt

# Code
COPY src/ ./src

# Tests
COPY tests/ ./tests

ENV PATH_TOWARDS_EFS=$efs_mount_path
ENV DEFAULT_REWARD_WINDOW_IN_SECONDS 7200

ARG test

RUN if [ "$test" = "true" ]; then \
        python3 -m pytest -v -s ; \
    else \
        python3 worker.py ; \
    fi

CMD [ "python", "./worker.py" ] 
