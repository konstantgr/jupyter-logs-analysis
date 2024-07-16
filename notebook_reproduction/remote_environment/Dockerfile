# Use an official Jupyter Notebook image as a parent image
FROM jupyter/minimal-notebook

# Set the working directory in the container to /app
WORKDIR /app

# Add current directory contents into the Docker image
ADD . /app

# Install poetry and configure to run without virtual environments.
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port for Jupyter Notebook
EXPOSE 8888

# Run Jupyter on container start
CMD ["start.sh", "jupyter", "notebook", "--NotebookApp.token=''", "--NotebookApp.notebook_dir=/app"]

# docker build -t agent-jupyter-interaction-docker-image .