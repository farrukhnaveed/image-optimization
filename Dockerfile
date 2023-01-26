FROM amd64/python:3.7-buster
WORKDIR /app
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y
RUN python -m pip install --upgrade pip
RUN pip install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu
# RUN python -m venv env
# CMD ["source", "env/bin/activate"]
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
COPY ./imageEnhancer /app/imageEnhancer
RUN cd imageEnhancer && python setup.py develop
RUN cd /app

EXPOSE 5000

# docker build -t image-processing .
# docker run -it --rm --name image-process -v $(pwd):/app image-processing bash
