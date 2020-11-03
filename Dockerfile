FROM python:3.7

WORKDIR /code

COPY ./requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Code
COPY src/ ./src

# Tests
COPY tests/ ./tests
COPY ./conftest.py .
COPY ./pytest.ini .

ENV PYTHONPATH=/code

# TEMPORARY COMMANDS TO TEST ONLINE
ENV PATH_TOWARDS_EFS /efs
ENV DEFAULT_REWARD_WINDOW_IN_SECONDS 7200
ENV AWS_BATCH_JOB_ARRAY_INDEX 1 
ENV JOIN_REWARDS_JOB_ARRAY_SIZE 3
ENV JOIN_REWARDS_REPROCESS_ALL false
RUN python src/create_test_env.py

CMD [ "python", "src/worker.py" ] 
