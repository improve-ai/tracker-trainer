FROM python:3.7

WORKDIR /code

COPY ./* ./
RUN python -m pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/code

CMD [ "python", "./join_rewards.py" ] 
