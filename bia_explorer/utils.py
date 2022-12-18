import numpy as np


def scale_to_uint8(array):

    scaled = array.astype(np.float32)

    if scaled.max() - scaled.min() == 0:
        return np.zeros(array.shape, dtype=np.uint8)

    scaled = 255 * (scaled - scaled.min()) / (scaled.max() - scaled.min())

    return scaled.astype(np.uint8)