#!/bin/bash

#SBATCH --job-name=transcribe         # Job name
#SBATCH --output=out.txt             # Standard output and error log (%j expands to job ID)
#SBATCH --partition=gpu              # Partition name (as seen in sinfo)
#SBATCH --time=24:00:00              # Max wall time (24 hours)

## Pick how many GPUs you need. Examples below. You need ONLY one of these lines
#SBATCH --gres=gpu:A6000:1            # EXAMPLE 1: Request 1 A100 GPU 

export CUDA_HOME=/usr/local/cuda-12.6
export PATH=/usr/local/cuda-12.6/bin:/opt/anaconda/bin:/usr/sbin:/usr/bin:/sbin:/bin
export LD_LIBRARY_PATH=/usr/local/cuda-12.6/lib64
export TORCH_CUDA_ARCH_LIST="8.0"
export FLASHINFER_COMPUTE_CAPS=80

# Run your application
cd /home/evywang/PodcastAd/code/
python transcribe.py /home/evywang/PodcastAd/infra/redownloaded_audio/ > /home/evywang/PodcastAd/infra/transcribe.txt