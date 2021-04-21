FROM python:3.8

WORKDIR /code

COPY ./* ./
RUN python -m pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/code
ENV PYTHONUNBUFFERED=TRUE

CMD [ "python", "./join_rewards.py" ] 
