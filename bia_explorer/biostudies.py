import logging
import pathlib
import datetime
from typing import List, Union, Optional

import requests
from pydantic import BaseModel, parse_raw_as


logger = logging.getLogger(__name__)


STUDY_URL_TEMPLATE = "https://www.ebi.ac.uk/biostudies/api/v1/studies/{accession}"
FLIST_URI_TEMPLATE = (
    "https://www.ebi.ac.uk/biostudies/files/{accession_id}/{flist_fname}"
)


class AttributeDetail(BaseModel):
    name: str
    value: str


class Attribute(BaseModel):
    name: str
    value: Optional[str]
    reference: bool = False
    nmqual: List[AttributeDetail] = []
    valqual: List[AttributeDetail] = []

    def as_tsv(self):
        if self.reference:
            tsv_rep = f"<{self.name}>\t{self.value}\n"
        else:
            tsv_rep = f"{self.name}\t{self.value}\n"

        return tsv_rep


class Link(BaseModel):
    url: str
    attributes: List[Attribute] = []

    def as_tsv(self):
        tsv_rep = "\n"
        tsv_rep += f"Link\t{self.url}\n"
        tsv_rep += "".join([attr.as_tsv() for attr in self.attributes])

        return tsv_rep


class Section(BaseModel):
    type: str
    accno: Optional[str]
    attributes: List[Attribute] = []
    subsections: List["Section"] = []
    links: List[Link] = []

    def as_tsv(self, parent_accno=None):
        tsv_rep = "\n"

        accno_str = self.accno if self.accno else ""
        if parent_accno:
            tsv_rep += f"{self.type}\t{accno_str}\t{parent_accno}"
        else:
            if self.accno:
                tsv_rep += f"{self.type}\t{accno_str}"
            else:
                tsv_rep += f"{self.type}"

        tsv_rep += "\n"

        tsv_rep += "".join([attr.as_tsv() for attr in self.attributes])
        tsv_rep += "".join([link.as_tsv() for link in self.links])
        tsv_rep += "".join([section.as_tsv(self.accno) for section in self.subsections])

        return tsv_rep


class Submission(BaseModel):
    accno: Optional[str]
    section: Section
    attributes: List[Attribute]

    def as_tsv(self) -> str:
        tsv_rep = f"Submission"
        if self.accno:
            tsv_rep += f"\t{self.accno}"
        tsv_rep += "\n"

        tsv_rep += "".join([attr.as_tsv() for attr in self.attributes])
        tsv_rep += self.section.as_tsv()

        return tsv_rep


# File List


class File(BaseModel):
    path: pathlib.Path
    size: int
    attributes: List[Attribute] = []


# API search classes


class StudyResult(BaseModel):
    accession: str
    title: str
    author: str
    links: int
    files: int
    release_date: datetime.date
    views: int
    isPublic: bool


class QueryResult(BaseModel):
    page: int
    pageSize: int
    totalHits: int
    isTotalHitsExact: bool
    sortBy: str
    sortOrder: str
    hits: List[StudyResult]


# API functions


def load_submission(accession_id: str) -> Submission:

    url = STUDY_URL_TEMPLATE.format(accession=accession_id)
    logger.info(f"Fetching submission from {url}")
    r = requests.get(url)

    assert r.status_code == 200

    submission = Submission.parse_raw(r.content)

    return submission


def attributes_to_dict(attributes: List[Attribute]) -> dict:

    return {attr.name: attr.value for attr in attributes}


def find_file_lists_in_section(section, flists) -> list:

    attr_dict = attributes_to_dict(section.attributes)

    if "File List" in attr_dict:
        flists.append(attr_dict["File List"])

    for subsection in section.subsections:
        find_file_lists_in_section(subsection, flists)

    return flists


def find_file_lists_in_submission(submission: Submission):

    return find_file_lists_in_section(submission.section, [])


def flist_from_flist_fname(accession_id: str, flist_fname: str):

    flist_url = FLIST_URI_TEMPLATE.format(
        accession_id=accession_id, flist_fname=flist_fname
    )

    r = requests.get(flist_url)
    logger.info(f"Fetching file list from {flist_url}")
    assert r.status_code == 200

    fl = parse_raw_as(List[File], r.content)

    return fl
