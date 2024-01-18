# Use the python image with a build argument for the version
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}

# update pip
RUN pip install --upgrade pip

# install some stuff to make interacting easier
RUN pip install ipython jupyterlab jupyter

# install dependencies to run tests in the container
RUN pip install pytest pytest-depends

# Copy the metacatalog package
RUN mkdir -p /app/metacatalog
COPY metacatalog /app/metacatalog
COPY MANIFEST.in /app/MANIFEST.in
COPY setup.py /app/setup.py
COPY README.md /app/README.md
COPY requirements.txt /app/requirements.txt

# Install the metacatalog package
WORKDIR /app
RUN pip install -e .
