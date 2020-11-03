FROM python:3.7

WORKDIR /code

COPY ./requirements.txt .

RUN python -m pip install -r requirements.txt

COPY src/ .

ENV PATH_TOWARDS_EFS=$efs_mount_path

ARG test

# https://stackoverflow.com/questions/43654656/dockerfile-if-else-condition-with-external-arguments
# docker build -t my_docker .  --build-arg arg=45
RUN if [ "$test" = "true" ]; then\
        pytest -v -s \
    else \
        python worker.py\
    fi

CMD [ "python", "./worker.py" ] 
