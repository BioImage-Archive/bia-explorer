from io import BytesIO
from typing import List

import pathlib
import requests
from pydantic import BaseModel
from PIL import Image

from bia_explorer.biostudies import load_submission, find_files_in_submission
from bia_explorer.models import FullBIAStudy


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
    size: int
    fpath: pathlib.Path
        
    def show(self):
        return self.show_pil()

    def show_pil(self):
        return load_pil_image_from_uri(self.uri)
        
        

class BIAStudy(BaseModel):
    images: List[BIAImage]

        
        
def load_bia_study(accession_id: str) -> BIAStudy:
    submission = load_submission(accession_id)
    
    # file_list_fnames = find_file_lists_in_submission(submission)
    # file_lists = [flist_from_flist_fname(accession_id, fname) for fname in file_list_fnames]
    # fl = file_lists[0]

    files = find_files_in_submission(submission)
    
    image_files = [file for file in files if is_image(file)]
    
    def get_image_uri(file):
        return FILE_TEMPLATE.format(accession_id=accession_id, file_relpath=file.path)
    
    images = [
        BIAImage(uri=get_image_uri(file), size=file.size, fpath=file.path)
        for file in image_files
    ]
    
    return BIAStudy(images=images)


def load_full_bia_study(accession_id: str) -> FullBIAStudy:

    uri = f"https://raw.githubusercontent.com/BioImage-Archive/bia-explorer/main/data/{accession_id}.json"

    r = requests.get(uri)

    return FullBIAStudy.parse_raw(r.content)