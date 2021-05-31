FROM apache/airflow:2.0.2
RUN pip install --upgrade --no-cache-dir pip
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir bs4
RUN pip install --no-cache-dir requests
RUN pip install --no-cache-dir pendulum
RUN pip install --no-cache-dir lxml