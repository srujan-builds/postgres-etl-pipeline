# Pull the base image
FROM python:3.8-slim

# Set the working directory to image
WORKDIR /app

# Copy contents to the wordir of image
COPY . /app

# Install the requirements
RUN pip install --no-cache-dir -r requirements.txt

# Running the command when a container is initialised
CMD [ "python", "script.py" ]

