# set base image (host OS)
FROM python:3.8

WORKDIR /code

COPY ./resources/requirements.txt .

RUN python -m pip install -r requirements.txt

COPY src/ .

ENV PATH_TOWARDS_EFS=$efs_mount_path

# This creates fake data in the input folder
RUN python create_test_env.py

CMD [ "python", "./worker.py" ] 
