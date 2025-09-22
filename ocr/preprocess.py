import cv2, numpy as np
from PIL import Image


# Preprocesos simples; podés ampliarlos luego (deskew, morphology, etc.)


def pil_to_cv(img_pil: Image.Image):
    return cv2.cvtColor(np.array(img_pil), cv2.COLOR_RGB2BGR)


def binarize(img_cv):
    gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
    thr = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 35, 11)
    return thr