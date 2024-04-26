from __future__ import annotations
from pydantic import BaseModel
import enum
from io import BytesIO
import requests
from PIL import Image

from bia_integrator_api import PublicApi
from bia_integrator_api.util import get_client
import bia_integrator_api.models as api_models

from collections.abc import Iterator
from typing import Optional, List
from pydantic import BaseModel

import dask.array as da
import zarr
import numpy as np
from ome_zarr.io import parse_url
from ome_zarr.reader import Reader

class ReprHtmlMixin(BaseModel):
    _include_fields: List[str] = [
        "context",
        "uuid",
        "title",
        "description",
        "authors",
        "name",
        "organism",
        "release_date",
        "accession_id",
        "imaging_type",
        "file_references_count",
        "images_count",
        "study_uuid",
        "original_relpath",
        "dimensions",
        "representations",
        "uri",
        "size",
        "type"
    ]

    def repr_html_embed_image(self) -> Optional[str]:
        """
        Override by children that support sample images (e.g. representative image, or thumbnail) 
        """
        return 

    def _repr_html_(self) -> str:
        def primitive_to_html(item):
            if isinstance(item, list):
                return list_to_html_table(item)
            elif isinstance(item, dict):
                return dict_to_html_table(item)
            else:
                return str(item)

        def list_to_html_table(obj_list):
            list_html = "<table>"

            for item in obj_list:
                list_html += f"<tr><td>{ primitive_to_html(item) }</td></tr>"

            list_html += "</table>"
            return list_html

        def dict_to_html_table(obj_comp):
            dict_html = "<table>"
            
            for k, v in obj_comp.items():
                if k not in self._include_fields:
                    continue

                dict_html += "<tr>"
                dict_html += f"<td>{k}</td>"
                dict_html += f"<td>{primitive_to_html(v)}</td>"
                dict_html += "</tr>"

            dict_html += "</table>"
            return dict_html

        html = ""
        if self.repr_html_embed_image():
            html += f"<img src=\"{self.repr_html_embed_image()}\" width=512 height=512 style=\"display: block; margin: auto\">"
        html += f"<table>{ primitive_to_html(self.dict()) }</table>"
        return html

class ImageRepresentationType(enum.Enum):
    OME_NGFF = "ome_ngff"
    OME_ZARR_ZIPPED = "zipped_zarr"

class BIACollection(api_models.BIACollection, ReprHtmlMixin):
    @classmethod
    def get_all(cls) -> Iterator[BIACollection]:
        for collection in ApiClient.all_collections():
            yield collection
    
    @classmethod
    def get_by_name(cls, collection_name: str) -> Optional[BIACollection]:
        return ApiClient.get_collection(collection_name)

    def get_studies(self) -> Iterator[BIAStudy]:
        for study_uuid in self.study_uuids:
            yield ApiClient.get_study_by_uuid(study_uuid)

class BIAStudy(api_models.BIAStudy, ReprHtmlMixin):
    @classmethod
    def get_all(cls) -> Iterator[BIAStudy]:
        for study in ApiClient.all_studies():
            yield study

    @classmethod
    def get_by_accession(cls, accession_id: str) -> Optional[BIAStudy]:
        return ApiClient.study_by_accession(accession_id)

    def repr_html_embed_image(self) -> Optional[str]:
        example_image_annotation = [
            annotation
            for annotation in self.annotations
            if annotation.key == 'example_image_uri'
        ]
        if not len(example_image_annotation):
            return
        
        example_image_annotation = example_image_annotation.pop()
        return example_image_annotation.value

    def get_images(self) -> Iterator[BIAImage]:
        for img in ApiClient.get_study_images(study_uuid = self.uuid):
            yield img
    
    def get_file_references(self) -> Iterator[FileReference]:
        for fileref in ApiClient.get_study_file_references(study_uuid=self.uuid):
            yield fileref
    
    def get_image_by_alias(self, alias) -> Optional[BIAImage]:
        """
        @TODO: Remove because aliases are deprecated?
        """
        return ApiClient.get_study_image_by_alias(accession_id = self.accession_id, alias = alias)

    def get_images_with_representation(self, representation: ImageRepresentationType) -> Iterator[BIAImage]:
        for img in ApiClient.get_study_images_with_representation(study_uuid = self.uuid, representation_type = representation):
            yield img

    def get_image_representations(self, representation_type: ImageRepresentationType) -> Iterator[BIAImageRepresentation]:
        for img in self.get_images_with_representation(representation_type):
            for representation_api in img.representations:
                if representation_api.type == representation_type.value:
                    representation = BIAImageRepresentation(**representation_api.dict(), image=img)

                    yield representation

    def get_file_references(self) -> Iterator[FileReference]:
        for fileref in ApiClient.get_study_file_references(study_uuid = self.uuid):
            yield fileref

class BIAImage(api_models.BIAImage, ReprHtmlMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.representations = [
            BIAImageRepresentation(**representation.dict())
            for representation in self.representations
        ]

    def get_study(self) -> BIAStudy:
        return ApiClient.get_study_by_uuid(self.study_uuid)

    def repr_html_embed_image(self) -> Optional[str]:
        example_image_annotation = [
            annotation
            for annotation in self.annotations
            if annotation.key == 'example_image_uri'
        ]
        if not len(example_image_annotation):
            return
        
        example_image_annotation = example_image_annotation.pop()
        return example_image_annotation.value


class ImageSlice(BaseModel):
    c: Optional[int] = None
    x: Optional[int] = None
    y: Optional[int] = None
    z: Optional[int] = None
    t: Optional[int] = None

    def as_tuple(self):
        """
        For numpy array slices from ImageSlice instances.
        Keep in mind that this *doesn't include any notion of data dimensions*,
            it just maps a human-friendly way of expressing slices into a fixed-order tuple, independent of the dimension order in the actual data
        """
        return (self.c, self.z, self.t)
        
class BIAImageRepresentation(api_models.BIAImageRepresentation, ReprHtmlMixin):
    """
    ! No representation-image link
    """
    def ome_ngff_to_dask_array(self):
        zgroup = zarr.open(self.uri[0], mode='r')
        zarray = zgroup['0']

        return da.from_zarr(zarray)

    def to_ome_zarr(self):
        reader = Reader(parse_url(self.uri[0]))
        
        return reader

    def zipped_zarr_to_dask_array(self):
        raise Exception("TODO")

    def to_bytesio(self, max_size_bytes = 100000000) -> BytesIO:
        # Always check size first
        file_size = requests.get(self.uri[0], stream=True).headers['Content-length']
        file_size = int(file_size)
        if max_size_bytes and file_size > max_size_bytes:
            raise Exception(f"File size {file_size} over the maximum limit of {max_size_bytes}. Pass a higher max_size_bytes (or None to disable) if your machine has enough memory")

        data_request = requests.get(self.uri[0])
        assert data_request.status_code == 200

        img = BytesIO(data_request.content)
        return img

    def to_dask_array(self):
        if self.type == ImageRepresentationType.OME_NGFF.value:
            return self.ome_ngff_to_dask_array()
        elif self.type == ImageRepresentationType.OME_ZARR_ZIPPED.value:
            return self.zipped_zarr_to_dask_array()
        else:
            raise Exception(f"No automatic display supported for representation of type {self.type} at '{self.uri[0]}'")

    def get_slice_image(self, image_slice: Optional[ImageSlice] = None, img_h = 512, img_w = None) -> Image:
        if image_slice is None:
            # @TODO: Make "default slice" some function of the actual darray 
            image_slice = ImageSlice(c=0,t=0,z=0)

        slice_tuple = image_slice.as_tuple()
        img_as_darray = self.to_dask_array()
        slice_to_display = img_as_darray[slice_tuple].compute()
        if len(np.shape(slice_to_display)) != 2:
            # This also guarantees all images are greyscale (they probably aren't in practice)
            raise Exception(f"Unable to display slice of shape {np.shape(slice_to_display)}. xy plane required")

        max_dimension_size = 5000
        if np.shape(slice_to_display)[0] > max_dimension_size or np.shape(slice_to_display)[1] > max_dimension_size:
            raise Exception(f"Trying to display a {np.shape(slice_to_display)} plane. Consider manual data manipulation and rendering for planes larger than ({max_dimension_size}, {max_dimension_size})")

        # len(np.shape(slice_to_display)) == 2 -> greyscale
        img = Image.fromarray(slice_to_display, 'L')
        if not img_h and not img_w:
            # no resizing
            return img
        else:
            # if any target dimension is None, keep aspect ratio
            if not img_w:
                img_w = img.height*img_h // img.width
            if not img_h:
                img_h = img.width*img_w // img.height

            return img.resize((img_h, img_w))

class FileReference(api_models.FileReference, ReprHtmlMixin):
    def get_study(self) -> BIAStudy:
        return ApiClient.get_study_by_uuid(self.study_uuid)

class ImageAcquisition(api_models.ImageAcquisition, ReprHtmlMixin):
    pass

class Biosample(api_models.Biosample, ReprHtmlMixin):
    pass

class Specimen(api_models.Specimen, ReprHtmlMixin):
    pass

class Biosample(api_models.Biosample, ReprHtmlMixin):
    pass

class ApiClient:
    """
    Utility functions on top of the generated api client.
    """

    # Use this directly if the wrapper gets in the way
    #   docs: https://45.88.81.209:8080/redoc#tag/public
    client: PublicApi = get_client()
    batch_size: int = 100
    
    @classmethod
    def all_studies(cls) -> Iterator[BIAStudy]:
        last_study_uuid = None
        fetched_any = True

        while fetched_any:
            fetched_any = False

            for study in cls.client.search_studies_exact_match(search_study_filter=api_models.SearchStudyFilter(
                start_uuid=last_study_uuid,
                limit = cls.batch_size
            )):
                fetched_any = True

                last_study_uuid = study.uuid
                yield study

    @classmethod
    def all_collections(cls) -> List[BIACollection]:
        collections = [
            BIACollection(**collection.dict())
            for collection in cls.client.search_collections()
        ]
        return collections
    
    @classmethod
    def get_collection(cls, collection_name: str) -> Optional[BIACollection]:
        matches = cls.client.search_collections(collection_name)
        if len(matches) == 0:
            return None
        elif len(matches) == 1:
            return BIACollection(**matches[0].dict())
        else:
            raise Exception("Unexpected multiple collections with the same name")

    
    @classmethod
    def study_by_accession(cls, accession_id: str) -> Optional[BIAStudy]:
        study_filter = api_models.SearchStudyFilter(
            study_match = api_models.SearchStudy(
                accession_id=accession_id
            ),
            limit=1
        )
        results = cls.client.search_studies_exact_match(search_study_filter=study_filter)

        if len(results):
            study = BIAStudy(**results[0].dict())
            return study
        return None
    
    @classmethod
    def get_study_images(cls, study_uuid) -> Iterator[BIAImage]:
        images_filter = api_models.SearchImageFilter(
            start_uuid = None,
            limit = cls.batch_size,
            study_uuid = study_uuid
        )
        images_fetched = cls.client.search_images_exact_match(images_filter)
        while len(images_fetched):
            for img in images_fetched:
                img = BIAImage(**img.dict())

                yield img
            
            images_filter.start_uuid = images_fetched[-1].uuid
            images_fetched = cls.client.search_images_exact_match(images_filter)
    
    @classmethod
    def get_study_image_by_alias(cls, accession_id, alias) -> Optional[BIAImage]:
        images = cls.client.get_study_images_by_alias(study_accession=accession_id, aliases=[alias])
        if len(images):
            img = images[0]
            img = BIAImage(**img.dict())
            return img
        else:
            return None


    @classmethod
    def get_study_file_references(cls, study_uuid) -> Iterator[FileReference]:
        filerefs_filter = api_models.SearchFileReferenceFilter(
            start_uuid = None,
            limit = cls.batch_size,
            study_uuid = study_uuid
        )
        filerefs_fetched = cls.client.search_file_references_exact_match(filerefs_filter)
        while len(filerefs_fetched):
            for fileref in filerefs_fetched:
                fileref = FileReference(**fileref.dict())

                yield fileref
            
            filerefs_filter.start_uuid = filerefs_fetched[-1].uuid
            filerefs_fetched = cls.client.search_file_references_exact_match(filerefs_filter)
    
    @classmethod
    def get_study_images_with_representation(cls, study_uuid: str, representation_type: ImageRepresentationType):
        images_filter = api_models.SearchImageFilter(
            start_uuid = None,
            limit = cls.batch_size,
            study_uuid = study_uuid,
            image_representations_any = [
                api_models.SearchFileRepresentation(
                    type=representation_type.value
                )
            ]
        )
        
        images_fetched = cls.client.search_images_exact_match(images_filter)
        while len(images_fetched):
            for img in images_fetched:
                img = BIAImage(**img.dict())

                yield img
            
            images_filter.start_uuid = images_fetched[-1].uuid
            images_fetched = cls.client.search_images_exact_match(images_filter)

    @classmethod
    def get_study_by_uuid(cls, study_uuid) -> BIAStudy:
        study = cls.client.get_study(study_uuid)
        study = BIAStudy(**study.dict())

        return study

    @classmethod
    def get_image_by_uuid(cls, img_uuid) -> BIAImage:
        img = cls.client.get_image(img_uuid)
        img = BIAImage(**img.dict())

        return img

    @classmethod
    def get_file_reference_by_uuid(cls, fileref_uuid) -> FileReference:
        fileref = cls.client.get_image(fileref_uuid)
        fileref = FileReference(**fileref.dict())

        return fileref
    