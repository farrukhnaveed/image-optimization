FROM amd64/python:3.7-buster
WORKDIR /app
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y
RUN python -m pip install --upgrade pip
# RUN python -m venv env
# CMD ["source", "env/bin/activate"]
RUN pip install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
CMD ["python", "setup.py", " develop"]

# docker build -t image-processing .
# docker run -it --rm --name image-process -v $(pwd):/app image-processing bash
# python inference_realesrgan.py -n RealESRGAN_x4plus -i 00003.png -o 00003_after.png
# python inference_realesrgan.py -n RealESRGAN_x4plus -i inputs
