from io import BytesIO
from typing import List

import requests
from pydantic import BaseModel
from imageio import imread_v2
from PIL import Image

from bia_explorer.biostudies import load_submission, find_file_lists_in_submission, flist_from_flist_fname


FILE_TEMPLATE = "https://www.ebi.ac.uk/biostudies/files/{accession_id}/{file_relpath}"


IMAGE_EXTS = ['.png','.jpg','.jpeg','.tif','.tiff']


def is_image(file):
    return file.path.suffix in IMAGE_EXTS


def load_pil_image_from_uri(image_uri):

    r = requests.get(image_uri)
    assert r.status_code == 200

    im = Image.open(BytesIO(r.content))

    return im


class BIAImage(BaseModel):
    uri: str
        
    def show(self):
        imarray = imread_v2(self.uri)
        return Image.fromarray(imarray)

    def show_pil(self):
        return load_pil_image_from_uri(self.uri)
        
        

class BIAStudy(BaseModel):
    images: List[BIAImage]
        
        
def load_bia_study(accession_id: str) -> BIAStudy:
    submission = load_submission(accession_id)
    
    file_list_fnames = find_file_lists_in_submission(submission)
    file_lists = [flist_from_flist_fname(accession_id, fname) for fname in file_list_fnames]
    fl = file_lists[0]

    
    image_files = [file for file in fl if is_image(file)]
    
    def get_image_uri(file):
        return FILE_TEMPLATE.format(accession_id=accession_id, file_relpath=file.path)
    
    images = [
        BIAImage(uri=get_image_uri(file))
        for file in image_files
    ]
    
    return BIAStudy(images=images)