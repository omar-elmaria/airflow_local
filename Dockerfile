FROM apache/airflow:2.3.4
# These are the new libraries that have to be installed
RUN pip3 install pandas numpy scipy matplotlib seaborn tqdm db-dtypes ipykernel ipywidgets ipython
# Install the libraries required for scraping
RUN pip3 install scrapy scrapy-playwright pyairtable scrapy-proxy-pool scrapy-user-agents scraperapi-sdk python-dotenv
# Install the playwright headless browsers
RUN playwright install
# Add some default paths
ENV PYTHONPATH="$PYTHONPATH:/opt/airflow/switchback_test_dag/py_scripts"
ENV PYTHONPATH="$PYTHONPATH:/opt/airflow/python_scrapy_airflow_pipeline"