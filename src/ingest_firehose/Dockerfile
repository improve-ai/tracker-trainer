FROM public.ecr.aws/lambda/python:3.8

WORKDIR /code

COPY ./requirements.txt ./requirements.txt

RUN python -m venv /code/venv && \
    source /code/venv/bin/activate && \
    pip3 install --upgrade pip --no-cache-dir && \
    pip3 install --no-cache-dir -r /code/requirements.txt && \
    rm -rf ~/.cache/pip /tmp/pip-ephem-wheel-cache*

COPY ./* ./

ENV PYTHONPATH="/code/:$PATH"
# make sure python from venv is visible and responds as default to python3 command
ENV PATH="/code/venv/bin/:$PATH"
ENV PYTHONUNBUFFERED=TRUE


ENTRYPOINT [ "python3", "worker.py" ]
