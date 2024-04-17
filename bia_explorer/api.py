from __future__ import annotations
from pydantic import BaseModel
import enum

from bia_integrator_api import PublicApi
from bia_integrator_api.util import get_client
import bia_integrator_api.models as api_models

from collections.abc import Iterator
from typing import Optional
from pydantic import BaseModel

import matplotlib.pyplot as plt

import dask.array as da
import zarr
import numpy as np

class ReprHtmlMixin(BaseModel):
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
                dict_html += "<tr>"
                dict_html += f"<td>{k}</td>"
                dict_html += f"<td>{primitive_to_html(v)}</td>"
                dict_html += "</tr>"

            dict_html += "</table>"
            return dict_html

        html = f"<table>{ primitive_to_html(self.dict()) }</table>"
        return html

class ImageRepresentationType(enum.Enum):
    OME_NGFF = "ome_ngff"
    OME_ZARR_ZIPPED = "zipped_zarr"

class BIAStudy(api_models.BIAStudy, ReprHtmlMixin):
    @classmethod
    def get_all(cls) -> Iterator[BIAStudy]:
        for study in ApiClient.all_studies():
            yield study

    @classmethod
    def get_by_accession(cls, accession_id: str) -> Optional[BIAStudy]:
        return ApiClient.study_by_accession(accession_id)

    def get_images(self) -> Iterator[BIAImage]:
        for img in ApiClient.get_study_images(study_uuid = self.uuid):
            yield img
    
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
    def get_study(self) -> BIAStudy:
        return ApiClient.get_study_by_uuid(self.study_uuid)

class ImageSlice(BaseModel):
    c: Optional[int] = None
    x: Optional[int] = None
    y: Optional[int] = None
    z: Optional[int] = None
    t: Optional[int] = None

    def as_tuple(self):
        # ?!
        return (self.c, self.z, self.t)
        
class BIAImageRepresentation(api_models.BIAImageRepresentation, ReprHtmlMixin):
    """
    ! No representation-image link
    """
    def ome_ngff_to_dask_array(self):
        zgroup = zarr.open(self.uri[0])
        zarray = zgroup['0']    

        return da.from_zarr(zarray)

    def zipped_zarr_to_dask_array(self):
        raise Exception("TODO")

    def to_dask_array(self):
        if self.type == ImageRepresentationType.OME_NGFF.value:
            return self.ome_ngff_to_dask_array()
        elif self.type == ImageRepresentationType.OME_ZARR_ZIPPED.value:
            return self.zipped_zarr_to_dask_array()
        else:
            raise Exception(f"No automatic display supported for representation of type {self.type} at '{self.uri[0]}'")

    def pyplot_display_slice(self, image_slice: Optional[ImageSlice] = None):
        if image_slice is None:
            # @TODO: Make "default slice" some function of the actual darray 
            image_slice = ImageSlice(c=0,t=0,z=0)

        slice_tuple = image_slice.as_tuple()
        img_as_darray = self.to_dask_array()
        slice_to_display = img_as_darray[slice_tuple].compute()
        if len(np.shape(slice_to_display)) != 2:
            raise Exception(f"Unable to display slice of shape {np.shape(slice_to_display)}. xy plane required")

        plt.imshow(slice_to_display, cmap='gray')
        plt.axis('off')
        plt.show()

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

            for study in cls.client.search_studies_exact_match(start_uuid=last_study_uuid, limit=cls.batch_size):
                fetched_any = True

                last_study_uuid = study.uuid
                yield study
    
    @classmethod
    def study_by_accession(cls, accession_id: str) -> Optional[BIAStudy]:
        study_filter = api_models.SearchStudyFilter(
            study_match = api_models.SearchStudy(
                accession_id=accession_id
            )
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
        print(images)
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
    