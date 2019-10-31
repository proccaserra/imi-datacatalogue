#!/usr/bin/env python3

from ccmm.dats.datsobj import DatsObj
from collections import OrderedDict
import ccmm.dats.util as util
import logging
import re
import sys


GTEX_DESCRIPTION = """GTEx provides a resource with which to study human gene expression and regulation and its relationship \
to genetic variation. It is funded by the NIH Common Fund. """

# List of GTEx studies cut and pasted from https://www.ncbi.nlm.nih.gov/gap/?term=phs000424
GTEX_STUDIES_STR = """
phs000424.v7.p2
Genotype-Tissue Expression (GTEx)Versions 1-7: passed embargo
VDAS752Tissue Expression, Reference SetLinks
HiSeq X Ten
"""
 
#To do: incorporate 
#HumanOmni5-Quad
#HumanOmni2.5
#Infinium HumanExome BeadChip
#HiSeq 2000
#HiSeq 2000
#GeneChip Human Gene 1.0 ST Array
#HiSeq 2000
#HiSeq X Ten

DBGAP_QUERY_URL_PREFIX = 'https://www.ncbi.nlm.nih.gov/gap/?term='
DBGAP_GTEX_QUERY_URL = DBGAP_QUERY_URL_PREFIX + 'phs000424'

HISEQ_TYPES = {
    "HiSeq 2000": util.get_annotation("Illumina HiSeq 2000"),
    "HiSeq X Ten": util.get_annotation("Illumina HiSeq X Ten"),
}

# TODO - duplicated from rnaseq_datasets.py
DB_GAP = DatsObj("DataRepository", [("name", "dbGaP")])

NIH_NHGRI = DatsObj("Organization", [
        ("name", "National Human Genome Research Institute"),
        ("abbreviation", "NHGRI")
        ])

# TODO - use DatsObjCache
cache = None
GTEX_V7_RNASEQ_TYPE = DatsObj("DataType", [
        ("information", util.get_annotation("transcription profiling", cache)),
        ("method", util.get_annotation("RNA-seq assay", cache)),
        ("platform", util.get_annotation("Illumina", cache))
        ])

GTEX_V7_WGS_TYPE = DatsObj("DataType", [
        ("information", util.get_annotation("DNA sequencing", cache)),
        ("method", util.get_annotation("whole genome sequencing assay", cache)),
        ("platform", util.get_annotation("Illumina", cache))
        ])

GTEX_V7_TYPES = [ GTEX_V7_WGS_TYPE, GTEX_V7_RNASEQ_TYPE ]


def get_dbgap_studies(qterm):
    studies = []
    study = None
    lnum  = 0

    # Add newline before each occurrence of "Versions" if not already present
    lines = []
    for line in GTEX_STUDIES_STR.split('\n'):
        m = re.match(r'^(\S+.*)(Versions?.*)$', line)
        if m is None:
            lines.append(line)
        else:
            lines.append(m.group(1))
            lines.append(m.group(2))

    for line in lines:
        lnum += 1
        # blank line
        if re.match(r'^\s*$', line):
            continue
        # study id
        m = re.match('^(phs\S+)$', line)
        if m is not None:
            study = { 'id': m.group(1) }
            studies.append(study)
            continue
        # study description
        m = re.match(r'^Genotype-Tissue Expression(.*)$', line)
        if m is not None:
            study['descr'] = m.group(1)
            continue
        # embargo release(s)
        m = re.match(r'^(Version.*)$', line)
        if m is not None:
            if 'versions' not in study:
                study['versions'] = []
            study['versions'].append(m.group(1))
            continue
        # details/participants/type of study
        m = re.match('^VDAS(\d+)(\D.*)Links$', line)
        if m is not None:
            study['n_participants'] = int(m.group(1))
            study['study_type'] = m.group(2)
            continue
        # platform
        m = re.match(r'^(HiSeq.*)$', line)
        if m is not None:
            study['platform'] = m.group(1)
            continue
        # parse error
        logging.fatal("unexpected content at line " + str(lnum) + " of dbGaP studies: " + line)
        sys.exit(1)

    n_studies = len(studies)
    logging.info("found " + str(n_studies) + " GTEx study in dbGaP")

    # convert studies to DATS Datasets
    datasets = []
    for s in studies:
        m = re.match(r'^phs\d+\.(v\d+)\.p\d+$', s['id'])
        if m is None:
            logging.fatal("unable to parse dataset/study version from study id " + s['id'])
            sys.exit(1)
        version = m.group(1)

        dimensions = [
            DatsObj("Dimension", [
                    ("name", { "value": "Actual Subject Count" } ),
                    ("description", "The actual number of subjects entered into a clinical trial."),
                    ("types", [ util.get_annotation("Actual Subject Number") ]),
                    ("values", [ s['n_participants'] ])
                    ])
            ]

        types = [OrderedDict([
            ("information", util.get_annotation("DNA sequencing")),
            ("method", util.get_annotation("whole genome sequencing assay")),
            ("platform", HISEQ_TYPES[s['platform']])
            ])]

        # TODO - Specify creators and release date(s) of this particular dataset.
        #  This may require parsing some of the metadata files and/or documents.
        # TODO - required field - using NIH NHLBI as placeholder, but need to revisit and assign specific study-level creator
        creators = [NIH_NHGRI]

        # TODO - find better location for study_type?
        extra_props = [ DatsObj("CategoryValuesPair", [("category", "study_type"), ("values", [s['study_type']])]) ]

        # Dataset
        dataset = DatsObj("Dataset", [
                ("identifier", DatsObj("Identifier", [("identifier", s['id'])])),
                ("version", version),
#                ("dates", []),
                #("title", s['descr']),
                ("title",  "Genotype-Tissue Expression Project (GTEx) WGS and RNA-Seq data"),
                ("storedIn", DB_GAP),
                ("types", types),
                ("creators", creators),
                ("dimensions", dimensions),
                ("extraProperties", extra_props)
#                ("producedBy", data_analysis),
 #               ("distributions", [DatsObj("DatasetDistribution", [
#                                ("access", DatsObj("Access", [
#                                            ("landingPage", GTEX_DATASETS_URL)
#                                            ]))
#                                ])]),
                ])

        datasets.append(dataset)
        
    return datasets

def get_dataset_json():
    # individual datasets corresponding to studies within GTEx
    data_subsets = [];

    # pull studies from dbGaP
    data_subsets = get_dbgap_studies("phs000424")

    # parent GTEx Dataset that represents the entire GTEx program
    parent_gtex_dataset = DatsObj("Dataset", [
            ("identifier", DatsObj("Identifier", [
                        # GTEx value - "GTEx_Analysis_2016-01-15_v7_RNA-SEQ"
                        ("identifier", "Genotype-Tissue Expression Project (GTEx)")
                        ])),
            ("title",  "Genotype-Tissue Expression Project (GTEx)"),
            ("description", GTEX_DESCRIPTION),
            ("storedIn", DB_GAP),
            ("types", GTEX_V7_TYPES),
            ("creators", [NIH_NHGRI]),
            ("distributions", [DatsObj("DatasetDistribution", [
                                    ("access", DatsObj("Access", [
                                                ("landingPage", DBGAP_GTEX_QUERY_URL)
                                                ]))
                                    ])]),
            ("hasPart", data_subsets)
            ])

    # TODO - add 'licenses', 'availability', 'dimensions', 'primaryPublications'?

    return parent_gtex_dataset

