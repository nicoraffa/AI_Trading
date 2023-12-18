FROM python:3.10-alpine

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache

COPY src/* ${LAMBDA_TASK_ROOT}

CMD ["Oracular.lambda_handler"]
