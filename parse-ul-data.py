__author__ = 'philippe rocca-serra'

from jsonschema import RefResolver, Draft4Validator, FormatChecker
from os import listdir
from os.path import isfile, join
import logging
import json
import os
import re
import time
import urllib.request
import urllib.parse
import requests
import pandas as pd
import codecs

# this needs to be installed by copying ccmm folder in this directory
# from https://github.com/dcppc/crosscut-metadata/tree/master/ccmm
from ccmm.dats.datsobj import DatsObj

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# print(os.path.dirname(__file__))

DATS_schemasPath = os.path.join(os.path.dirname(__file__), "../../../../DATS/dats-tools/json-schemas")
DATS_contextsPath = os.path.join(os.path.dirname(__file__), "../../../../DATS/dats-tools/json-contexts")

print("DATS:", DATS_schemasPath)

def get_schemas_store(path):
    """

    :param path: a string,
    :return: an array, which holds all the json schemas defining the model
    """
    files = [f for f in listdir(path) if isfile(join(path, f))]
    store = []
    for schema_filename in files:
        schema_path = os.path.join(DATS_schemasPath, schema_filename)
        print(schema_filename)
        with open(schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
            store.append({schema['id']:  schema})

    return store


if __name__ == '__main__':

    root_dir = os.path.dirname(os.path.realpath(__file__))
    print("ROOT: ", root_dir)
    output_dir = '../output/'

    INPUT_DC = "/Users/philippe/Documents/IMI-FAIR+/WP1-Data-Catalogue/IMIPROJECTS.csv"
    df = pd.read_csv(INPUT_DC)

    try:

        # id = DatsObj("Identifier", [("identifier", "https://fairsharing.org/FAIRsharing.xfrgsf"),
        #                             ("identifierSource", "FAIRSHARING")])

        for i in range(0, len(df)):

            if df["StartDate"][i] != "-":
                start_date = DatsObj("Date", [("date", df["StartDate"][i]),
                                                ("type", DatsObj("Annotation", [("value", "start date"),
                                                                                ("valueIRI", "")]))
                                                ])
            else:
                start_date = DatsObj("Date", [("date", "None"),
                                                ("type", DatsObj("Annotation", [("value", "start date"),
                                                                                ("valueIRI", "")]))
                                                ])

            # print(start_date)

            if df["EndDate"][i] != "-":
                end_date = DatsObj("Date", [("date", df["EndDate"][i]),
                                                ("type", DatsObj("Annotation", [("value", "end date"),
                                                                                ("valueIRI", "")]))
                                                ])
            else:
                end_date = DatsObj("Date", [("date", "None"),
                                                ("type", DatsObj("Annotation", [("value", "end date"),
                                                                                ("valueIRI", "")]))
                                                ])

            d_kwds = []
            if df["Keywords"][i] != "" and not isinstance(df["Keywords"][i], float):
                kwds = df["Keywords"][i].split(":")
                for kwd in kwds:
                    d_kwd = DatsObj("Annotation", [("value", kwd), ("valueIRI", "")])
                    d_kwds.append(d_kwd)

            IMI_funder = DatsObj("Organization", [

                            ("name", "IMI")
                    ])
            print(IMI_funder)

            d_grant = ""
            if df["GrantAgreementNo"][i] != "":
                d_grant = DatsObj("Grant", [
                    ("name", "IMI grant #:" + str(df["GrantAgreementNo"][i]))
                     ])

            d_orgs = []
            if not isinstance(df["EFPIAcompanies"][i], float):
                efpia_orgs = df["EFPIAcompanies"][i].split(':')
                for org in efpia_orgs:
                   d_org = DatsObj("Organization", [("name", org)])
                   d_orgs.append(d_org)


            if df["Project Coordinator Name"][i] != "" and df["Project Contact  email"][i] != "" :

                person_bits = df["Project Coordinator Name"][i].split(",")
                d_person = DatsObj("Person",[
                        ("fullName", person_bits[0]),
                        ("email", df["Project Contact  email"][i]),
                        ("affiliations",[person_bits[1]])
                            ])

            imi_project = DatsObj("Dataset", [
                ("identifier", DatsObj("Identifier", [("identifier", "IMI-Cat#" + str(i))])),
                ("title", df["Project Acronym"][i]),
                ("description", df["ShortDescription"][i] + ". SUMMARY: " + df["Summary"][i]),
                ("distributions", []),
                ("creators", [d_orgs,d_person]),
                ("keywords", d_kwds),
                ("dates", [start_date,end_date]),
                ("types", []),
                ("producedBy", []),
                ("storedIn", ""),
                ("isAbout", []),
                ("version", "")
               ])

            # imi_project["dates"].append(start_date)
            # imi_project["dates"].append(end_date)

            print("IMI PROJECT:", imi_project)

    except IOError as ioe:
        print(ioe)

