# 1. Use the NVIDIA PyTorch Base
FROM nvcr.io/nvidia/pytorch:24.12-py3

# 2. Set working directory
WORKDIR /workspace

# 3. Install torchmonarch
RUN pip install torchmonarch-nightly

# 4. Copy your worker script into the container
COPY worker.py /workspace/worker.py

# 5. Set the default command to run your script
# Using "python -u" forces unbuffered output so logs show up instantly in kubectl
CMD ["python", "-u", "worker.py"]
