FROM python:3.7

ENV PYTHONUNBUFFERED 1

RUN mkdir /prod_env

WORKDIR /prod_env

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . /prod_env/

CMD ["python" , "./app.py"]