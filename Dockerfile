FROM python:3.9-slim

RUN apt-get update && apt-get upgrade -y

RUN pip install -U pip

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

ENV DB_URI='postgresql://postgres:novo_db_password$@postgresql/novo_task_db'
ENV RESULTS_DIR=/results

RUN chmod +x run_scripts.sh

CMD ["./run_scripts.sh"]
