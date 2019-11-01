__author__ = 'philippe rocca-serra'

from jsonschema import RefResolver, Draft4Validator, FormatChecker
from os import listdir
from os.path import isfile, join
import logging
import json
import os
import uuid
import re
import time
import urllib.request
import urllib.parse
import requests
import pandas as pd
import codecs

# this needs to be installed by copying ccmm folder in this directory
# from https://github.com/dcppc/crosscut-metadata/tree/master/ccmm
from ccmm.dats.datsobj import DatsObj, DATSEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# print(os.path.dirname(__file__))

DATS_schemasPath = os.path.join(os.path.dirname(__file__), "../../../../DATS/dats-tools/json-schemas")
DATS_contextsPath = os.path.join(os.path.dirname(__file__), "../../../../DATS/dats-tools/json-contexts")

# print("DATS:", DATS_schemasPath)


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


def validate_instance(path, filename, schema_filename, error_printing):
    """

    :param path:
    :param filename:
    :param schema_filename:
    :param error_printing:
    :return:
    """
    try:
        schema_file = open(join(DATS_schemasPath, schema_filename))
        schema = json.load(schema_file)
        schemastore = get_schemas_store(DATS_schemasPath)
        print(schemastore)
        store = {schema['id']: schema}
        resolver = RefResolver(base_uri='file://' + DATS_schemasPath + '/' + schema_filename, referrer=schema,
                               store=store)
        validator = Draft4Validator(schema, resolver=resolver, format_checker=FormatChecker())
        logger.info("Validating %s against %s ", filename, schema_filename)

        try:
            instance_file = open(join(path, filename))
            instance = json.load(instance_file)

            if error_printing:
                errors = sorted(validator.iter_errors(instance), key=lambda e: e.path)
                for error in errors:
                    print(error.message)

                if len(errors) == 0:
                    return True
                else:
                    return False

            elif error_printing == 0:
                errors = sorted(validator.iter_errors(instance), key=lambda e: e.path)
                for error in errors:
                    for suberror in sorted(error.context, key=lambda e: e.schema_path):
                        print(list(suberror.schema_path), suberror.message)

                if len(errors) == 0:
                    logger.info("...done")
                    return True
                else:
                    return False
            else:
                try:
                    validator.validate(instance, schema)
                    logger.info("...done")
                    return True
                except Exception as e:
                    logger.error(e)
                    return False
        except IOError as ioe:
            print(ioe)
            # finally:
        #     instance_file.close()
    except IOError as ioe2:
        print(ioe2)

    finally:
        schema_file.close()


def validate_dataset(path, filename, error_printing):
    """

    :param path: string,
    :param filename: string, the name of the json instance file
    :param error_printing: int, 0 or 1 for suppressing or allowing output respectively
    :return:
    """
    return validate_instance(path, filename, "dataset_schema.json", error_printing)


def validate_schema(path, schemaFile):
    """

    :param path:
    :param schemaFile:
    :return:
    """
    try:
        logger.info("Validating schema %s", schemaFile)
        schema_file = open(join(path, schemaFile))
        schema = json.load(schema_file)
        try:
            Draft4Validator.check_schema(schema)
            return True
        except Exception as e:
            logger.error(e)
            return False
        logger.info("done.")
    finally:
        schema_file.close()


def validate_schemas(path):
    """

    :param path:
    :return:
    """
    result = True
    files = [f for f in listdir(path) if isfile(join(path, f))]
    for schemaFile in files:
        result = result and validate_schema(path, schemaFile)
    return result


def validate_dats_schemas():
    """

    :return:
    """
    return validate_schemas(DATS_schemasPath)


def inject_context(instance, schema_name):
        """
        Transform a DATS JSON into a JSON-LD by injecting @context and @type keywords
        :return: a JSON-LD of the DATS JSON
        """

        mapping_url = "dats_context_mapping.json"
        base_schema = "dataset_schema.json"
        storage = {}
        with open(mapping_url, "r") as mappings:
            # print("mapping_url: ", mapping_url)
            context_mapping = json.load(mappings)["contexts"]

            print("mapping: ", context_mapping)
            main_context_url = context_mapping[base_schema]
            print("main context url: ", main_context_url)

            with open(instance, "r") as study_instance_file:
                study_instance = json.load(study_instance_file)

                study_instance["@context"] = main_context_url
                study_instance["@type"] = schema_name.lower() + "_schema.json"
                print("@Type: ", study_instance["@type"])

                if study_instance["@type"] not in storage:
                    print(context_mapping[study_instance["@type"]])
                    schema_response = requests.get(context_mapping[study_instance["@type"]])
                    print("RESPONSE: ",  schema_response)

                    response_json_context = schema_response.json()
                    print("RESPONSE_JSON: ", response_json_context)
                    # schema = response_json['data']
                    # print(response_json['data'])
                    # schema = json.load(response["data"])

                    storage[study_instance["@type"]] = response_json_context

                for field in study_instance:

                    if type(study_instance[field]) == list:
                        prop = field + "_schema.json"
                        if prop in context_mapping.keys():
                            print("field:", field, "| ", prop)
                            for item in study_instance[field]:
                                item["@context"] = context_mapping[prop]
                                item["@type"] = field.capitalize()
                    elif type(study_instance[field]) == dict:
                        prop = field + "_schema.json"
                        study_instance[field]["@context"] = context_mapping[prop]
                        study_instance[field]["@type"] = field.capitalize()

                print(study_instance)
                return study_instance



if __name__ == '__main__':

    root_dir = os.path.dirname(os.path.realpath(__file__))
    print("ROOT: ", root_dir)
    output_dir = '../output/'

    INPUT_DC = "./input/IMIPROJECTS.csv"
    df = pd.read_csv(INPUT_DC)

    try:

        # id = DatsObj("Identifier", [("identifier", "https://fairsharing.org/FAIRsharing.xfrgsf"),
        #                             ("identifierSource", "FAIRSHARING")])

        repo = DatsObj("DataRepository", [("name","Elixir IMI Data Catalogue"),
                                          ("description",
                                          "A catalogue of European Union Innovative \
Medicine projects and their associated datasets"),
                                          ("access", DatsObj("Access", [("landingPage",
                                                                         "https://datacatalog.elixir-luxembourg.org/"),
                                                                        ("accessURL",
                                                                         "https://datacatalog.elixir-luxembourg.org/")])
                                           )
                                          ])
        imi_projects = []

        IMI_funder = DatsObj("Organization", [("name", "IMI")])

        IMI_catalogue_distribution = DatsObj("DatasetDistribution", [("name", "IMI catalogue distribution"),
                                                              ("conformsTo", DatsObj("DataStandard",
                                                                                    [("name", "DATS")])),
                                                              ])

        imi_project_catalogue = DatsObj("Dataset", [
            ("identifier", DatsObj("Identifier", [("identifier", "IMI Cat#" + str(uuid.uuid4()))])),
            ("title", "A collection of European Union Innovative \
Medicine projects and their associated datasets"),
            ("description", "Data Catalogue will directly impact the range of sources and ease with which projects can\
access data sources. With the eTRIKS Data Catalogue, researchers will be able to create awareness and \
recognition of their data contribution and demonstrate value of partner projects. Users will have the \
opportunity to find and access selected datasets. The repository will fuel dissemination of results \
with better outcomes for research projects as well as driving success for patients in the medical \
landscape. \
The support from IMI having new and retrospective projects to suggest filling in the catalogue will be\
of great value to leverage the benefits for all stakeholders."),
            ("distributions", []),
            ("creators", [IMI_funder]),
            ("keywords", []),
            ("dates", []),
            ("types", []),
            ("producedBy", []),
            ("hasPart", imi_projects),
            ("storedIn", repo),
            ("isAbout", []),
            ("version", "0.1"),
            ("isAbout", []),
            ("distributions", [IMI_catalogue_distribution]),
            ("extraProperties", [])
        ])

        # print("catalogue: ", imi_project_catalogue)

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

            d_grant = ""
            if df["GrantAgreementNo"][i] != "":
                d_grant = DatsObj("Grant", [
                    ("name", "IMI grant #:" + str(df["GrantAgreementNo"][i])),
                    ("extraProperties", [])
                     ])

            grant_extra_props = []
            if df["EFPIAFunding"][i] != "":
                efpia_funds = DatsObj("CategoryValuesPair", [("category", "EFPIA funding"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", str(df["EFPIAFunding"][i])),
                                                                               ("valueIRI", "")])])])
                grant_extra_props.append(efpia_funds)

            if df["IMIFunding"][i] != "":
                imi_funds = DatsObj("CategoryValuesPair", [("category", "IMI funding"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", str(df["IMIFunding"][i])),
                                                                               ("valueIRI", "")])])])
                grant_extra_props.append(imi_funds)

            if df["OtherFunding"][i] != "":
                other_funds = DatsObj("CategoryValuesPair", [("category", "Other funding"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", str(df["OtherFunding"][i])),
                                                                               ("valueIRI", "")])])])
                grant_extra_props.append(other_funds)

            if df["TotalCost"][i] != "":
                other_funds = DatsObj("CategoryValuesPair", [("category", "Total Cost"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", str(df["TotalCost"][i])),
                                                                               ("valueIRI", "")])])])
                grant_extra_props.append(other_funds)

            d_orgs = []

            # DEALING WITH EFPIA
            if not isinstance(df["EFPIAcompanies"][i], float):

                role = DatsObj("Annotation", [("value", "EFPIA partner"),
                                              ("valueIRI", "")])

                efpia_orgs = df["EFPIAcompanies"][i].split(':')

                for org in efpia_orgs:
                    d_org = DatsObj("Organization", [("name", org), ("roles", [role])])
                    d_orgs.append(d_org)

            # DEALING WITH UNIVERSITIES
            if not isinstance(df["Univerisities"][i], float):

                role = DatsObj("Annotation", [("value", "University"),
                                              ("valueIRI", "")])

                uni_orgs = df["Univerisities"][i].split(':')

                for org in uni_orgs:
                    d_org = DatsObj("Organization", [("name", org), ("roles", [role])])
                    d_orgs.append(d_org)

            # DEALING WITH SMES
            if not isinstance(df["SMEs"][i], float):

                role = DatsObj("Annotation", [("value", "SME"),
                                              ("valueIRI", "")])

                sme_orgs = df["SMEs"][i].split(':')

                for org in sme_orgs:
                    d_org = DatsObj("Organization", [("name", org), ("roles", [role])])
                    d_orgs.append(d_org)

            # DEALING WITH PATIENT ORGs
            if not isinstance(df["PatientOrganisations"][i], float):

                role = DatsObj("Annotation", [("value", "Patient Organisations"),
                                              ("valueIRI", "")])

                patient_orgs = df["PatientOrganisations"][i].split(':')

                for org in patient_orgs:
                    d_org = DatsObj("Organization", [("name", org), ("roles", [role])])
                    d_orgs.append(d_org)

            # DEALING WITH PATIENT ORGs
            if not isinstance(df["ThirdParties"][i], float):

                role = DatsObj("Annotation", [("value", "Third Parties"),
                                              ("valueIRI", "")])

                thirdp_orgs = df["ThirdParties"][i].split(':')

                for org in thirdp_orgs:
                    d_org = DatsObj("Organization", [("name", org), ("roles", [role])])
                    d_orgs.append(d_org)

            # DEALING WITH PARTNERS
            if not isinstance(df["Partners"][i], float):

                role = DatsObj("Annotation", [("value", "Partners"),
                                              ("valueIRI", "")])

                partners_orgs = df["Partners"][i].split(':')

                for org in partners_orgs:
                    d_org = DatsObj("Organization", [("name", org), ("roles", [role])])
                    d_orgs.append(d_org)

            if df["Project Coordinator Name"][i] != "" and df["Project Contact  email"][i] != "":

                person_bits = df["Project Coordinator Name"][i].split(",")
                d_person = DatsObj("Person",[
                        ("fullName", person_bits[0]),
                        ("email", df["Project Contact  email"][i]),
                        ("affiliations",[person_bits[1]])
                            ])

            dataset_extra_props = []

            if df["IMIProgram"][i] != "":
                imi_prog = DatsObj("CategoryValuesPair", [("category", "IMI Program"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", df["IMIProgram"][i]),
                                                                               ("valueIRI", "")])])])
                dataset_extra_props.append(imi_prog)

            if df["IMICall"][i] != "":
                imi_call = DatsObj("CategoryValuesPair", [("category", "IMI Call"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", str(df["IMICall"][i])),
                                                                               ("valueIRI", "")])])])
                dataset_extra_props.append(imi_call)

            if df["Project Status Group (based on End Date)"][i] != "":
                imi_status = DatsObj("CategoryValuesPair", [("category", "Project Status Group"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", df["Project Status Group (based on End Date)"][i]),
                                                                               ("valueIRI", "")])])])
                dataset_extra_props.append(imi_status)

            if df["TypeOfAction"][i] != "":
                imi_toa = DatsObj("CategoryValuesPair", [("category", "Type of Action"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", df["TypeOfAction"][i]),
                                                                               ("valueIRI", "")])])])
                dataset_extra_props.append(imi_toa)

            if df["FAIRification"][i] != "":
                fairified = DatsObj("CategoryValuesPair", [("category", "FAIRification"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", df["FAIRification"][i]),
                                                                               ("valueIRI", "")])])])
                dataset_extra_props.append(fairified)

            if df["FAIRplus: responsible public partner"][i] != "":
                fairplus_resp = DatsObj("CategoryValuesPair", [("category", "FAIRplus: responsible public partner"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", df["FAIRplus: responsible public partner"][i]),
                                                                               ("valueIRI", "")])])])
                dataset_extra_props.append(fairplus_resp)

            if df["FAIRplus: responsible EFPIA partner"][i] != "":
                fairplus_efpia_resp = DatsObj("CategoryValuesPair", [("category", "FAIRplus: responsible EFPIA partner"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", df["FAIRplus: responsible EFPIA partner"][i]),
                                                                               ("valueIRI", "")])])])
                dataset_extra_props.append(fairplus_efpia_resp)

            if df["EFPIA project lead"][i] != "":
                fairplus_efpia_resp = DatsObj("CategoryValuesPair", [("category", "EFPIA project lead"),
                                                          ("categoryIRI", ""),
                                                          ("values", [DatsObj("Annotation",
                                                                              [("value", df["EFPIA project lead"][i]),
                                                                               ("valueIRI", "")])])])
                dataset_extra_props.append(fairplus_efpia_resp)

            imi_project = DatsObj("Dataset", [
                ("identifier", DatsObj("Identifier", [("identifier", "IMI-Cat#" + str(i))])),
                ("title", df["Project Acronym"][i]),
                ("description", df["ShortDescription"][i] + ". SUMMARY: " + df["Summary"][i]),
                ("distributions", []),
                ("creators", [d_orgs, d_person]),
                ("keywords", d_kwds),
                ("dates", [start_date,end_date]),
                ("types", []),
                ("producedBy", []),
                ("storedIn", ""),
                ("isAbout", [d_kwds]),
                ("version", ""),
                ("isAbout", []),
                ("extraProperties", [dataset_extra_props])
               ])

            imi_projects.append(imi_project)

        imi_project_catalogue.set("hasPart", imi_projects)

        DATSEncoder().encode(imi_project_catalogue)
        # imi_data_jstr = json.dumps(cls=DATSEncoder)

        print(imi_project_catalogue.toJSON())
        # imi_data_jstr = json.dumps(imi_project_catalogue.__dict__)

        # print("FULL CATALOGUE:", imi_data_jstr)

        # script_dir = os.path.dirname(__file__)
        # print("DIR", script_dir)
        filename = 'IMI_datacatalogue_as_DATS.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(imi_project_catalogue.toJSON(), f)

        # print("validating: ", script_dir, filename)
        # validate_dataset(script_dir, filename, 1)

        # script_dir = os.path.dirname(__file__)
        # this_instance = inject_context(filename, "Dataset")
        # with open("IMI_datacatalogue_as_DATS.jsonld", 'w', encoding='utf-8') as otherf:
        #     json.dump(this_instance, otherf, ensure_ascii=False, indent=4)

    except IOError as ioe:
        print(ioe)

