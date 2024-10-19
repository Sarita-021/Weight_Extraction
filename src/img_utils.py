import os
import pandas as pd 
import multiprocessing
import time
import requests
from time import time as timer
from tqdm import tqdm
import numpy as np
from pathlib import Path
from functools import partial
import urllib
from PIL import Image


def create_placeholder_image(image_save_path):
    try:
        placeholder_image = Image.new('RGB', (100, 100), color='black')
        placeholder_image.save(image_save_path)
    except Exception as e:
        return

def download_image(image_link, filename, save_folder, retries=3, delay=3):
    if not isinstance(image_link, str):
        return

    image_save_path = os.path.join(save_folder, filename)

    if os.path.exists(image_save_path):
        return

    for _ in range(retries):
        try:
            urllib.request.urlretrieve(image_link, image_save_path)
            return
        except:
            time.sleep(delay)
    
    create_placeholder_image(image_save_path) #Create a black placeholder image for invalid links/images


def download_image_with_filename(args, save_folder, retries, delay):
    image_link, group_id = args
    group_id = str(group_id)
    file_extension = Path(image_link).suffix  
    filename = f"{group_id}{file_extension}"
    download_image(image_link, filename=filename, save_folder=save_folder, retries=retries, delay=delay)


def download_images(image_links, group_ids, download_folder, allow_multiprocessing=True):
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    if allow_multiprocessing:
        download_image_partial = partial(
            download_image_with_filename, save_folder=download_folder, retries=3, delay=3)

        with multiprocessing.Pool(64) as pool:
            list(tqdm(pool.imap(download_image_partial, zip(image_links, group_ids)), total=len(image_links)))
            pool.close()
            pool.join()
    else:
        for image_link, group_id in tqdm(zip(image_links, group_ids), total=len(image_links)):
            group_id = str(group_id)
            download_image(image_link, filename=group_id, save_folder=download_folder, retries=3, delay=3)
        