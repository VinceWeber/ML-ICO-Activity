import torch
if torch.cuda.is_available():
    device = torch.device("cuda")
else:
    device = torch.device("cpu")
print("Using", device, "device")


import tensorflow as tf
print("Num GPUs Available: ", len(tf.config.list_physical_devices('GPU')))