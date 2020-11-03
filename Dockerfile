FROM python:3.8

WORKDIR /code

COPY ./resources/requirements.txt .

RUN python -m pip install -r requirements.txt

COPY src/ .

ENV PATH_TOWARDS_EFS=$efs_mount_path

# This creates fake data in the input folder
RUN python create_test_env.py

ARG test

# https://stackoverflow.com/questions/43654656/dockerfile-if-else-condition-with-external-arguments
# docker build -t my_docker .  --build-arg arg=45
RUN if [ "$test" = "true" ]; then\
        pytest
    else \
        python worker.py\
    fi

CMD [ "python", "./worker.py" ] 
