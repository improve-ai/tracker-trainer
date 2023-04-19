# syntax=docker/dockerfile:1.2
FROM public.ecr.aws/lambda/python:3.8

RUN yum update -y && \
    yum update -y libxml2 kernel && \
    yum install amazon-linux-extras gcc git python3-devel -y --setopt=install_weak_deps=False && \
    yum clean metadata &&  \
    yum autoremove && yum clean all && \
    rm -rf /var/cache/yum/*

COPY ./requirements.txt /code/requirements.txt

# this solves root privilige warnings thrown by pip
RUN python -m venv /code/venv && \
    source /code/venv/bin/activate && \
    pip3 install --upgrade pip --no-cache-dir && \
    pip3 install --no-cache-dir -r /code/requirements.txt && \
    rm -rf ~/.cache/pip /tmp/pip-ephem-wheel-cache*

COPY ./dask_config/config.yaml /etc/dask/config.yaml

COPY ./code/ /code

# dump out to terminal / docker logs
ENV PYTHONUNBUFFERED=TRUE
# do not create *.pyc files
ENV PYTHONDONTWRITEBYTECODE=TRUE

# pythonpath needs to be appended so python can find trainer code and submodules when calling
# `train`
ENV PYTHONPATH="/code:${PYTHONPATH}"
# make sure python from venv is visible and responds as default to python3 command
ENV PATH="/code/venv/bin/:$PATH"

WORKDIR /code
# make sure that python3 is invoked as an entrypoint
ENTRYPOINT [ "python3" ]