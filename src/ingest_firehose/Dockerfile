FROM public.ecr.aws/lambda/python:3.8

WORKDIR /code

COPY ./* ./

RUN yum install git gcc -y --setopt=install_weak_deps=False && \
    yum autoremove && yum clean all && \
    rm -rf /var/cache/yum/* && \
    python -m pip install --no-cache-dir --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt && \
    rm -rf ~/.cache/pip


ENV PYTHONPATH=/code
ENV PYTHONUNBUFFERED=TRUE


ENTRYPOINT [ "python3", "worker.py" ]
