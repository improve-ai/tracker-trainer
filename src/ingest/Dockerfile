FROM public.ecr.aws/lambda/python:3.8

# yum update glib2 libssh2 -> python3.8 image from public ECR contains versions
#  with vulnerabilities
RUN yum update -y && \
    yum update -y glib2 libssh2 && \
    yum clean metadata &&  \
    yum autoremove && yum clean all && \
    rm -rf /var/cache/yum/*

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY ./* ./

ENV PYTHONUNBUFFERED=TRUE

CMD [ "ingest_firehose.lambda_handler" ]