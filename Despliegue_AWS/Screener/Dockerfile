FROM public.ecr.aws/lambda/python:3.11

# Instala las bibliotecas de desarrollo de libxml2 y libxslt
RUN yum install -y libxml2-devel libxslt-devel

# Instala las dependencias de desarrollo de otras bibliotecas
RUN yum install -y gcc

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy all files in ./src
COPY src/* ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler.
CMD [ "Screener.lambda_handler" ]

