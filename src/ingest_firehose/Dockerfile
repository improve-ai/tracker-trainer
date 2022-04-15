FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY ./* ./

ENV PYTHONUNBUFFERED=TRUE

CMD [ "ingest_firehose.lambda_handler" ]