# Use Amazon Linux 2023 with Python 3.11
FROM public.ecr.aws/lambda/python:3.11

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Install dependencies
RUN pip install --no-cache-dir numpy scipy joblib geopy scikit-learn pandas 

# Command for Lambda
CMD ["lambda_function.lambda_handler"]