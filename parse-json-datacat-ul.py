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

from ccmm.dats.datsobj import DatsObj, DATSEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOCAL =  os.path.dirname(__file__)
# print(os.path.dirname(__file__))
print("LOCAL ", LOCAL)

DATS_schemasPath = os.path.join(LOCAL, "../DATS/dats-tools/json-schemas")
DATS_contextsPath = os.path.join(os.path.dirname(__file__), "../DATS/dats-tools/json-contexts")

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
    output_dir = './output/'

    INPUT_DC = "./input/records.json"

    with open(INPUT_DC) as json_doc:
        data = json.load(json_doc)
        for record in data['docs']:
            if "tags" in record.keys():
                for tag in record['tags']:
                    print("tag:", tag)
            else:
                print("NO TAGS")

            if "study_type" in record.keys():
                s_types = record['study_type'].split(';')
                for type in s_types:
                    print("study_type:", type.lstrip())

            if "subjects_number_per_cohort" in record.keys():
                if "; " in record['subjects_number_per_cohort']:
                    subjects_per_cohorts = record['subjects_number_per_cohort'].split('; ')
                    for subject_per_cohort in subjects_per_cohorts:
                        print(subject_per_cohort.lstrip())
                # if ": " in record['subjects_number_per_cohort']:
                #     subjects_per_cohorts = record['subjects_number_per_cohort'].split(': ')
                #     for subject_per_cohort in subjects_per_cohorts:
                #         print(subject_per_cohort.lstrip())
                if "\n" in record['subjects_number_per_cohort']:
                    cohorts = record['subjects_number_per_cohort'].split('\n')
                    for cohort in cohorts:
                        print("cohort: ", cohort.lstrip())

            if "secondary_analysis" in record.keys():
                if ": " in record['secondary_analysis']:
                    assays = record['secondary_analysis'].split(': ')
                    for assay in assays:
                        print("assay: ", assay.lstrip())
                elif ";" in record['secondary_analysis']:
                    assays = record['secondary_analysis'].split(';')
                    for assay in assays:
                        print("assay: ", assay.lstrip())

            if "reference_publications" in record.keys():
                if "DOI:" in record['reference_publications']:
                    refs = record['reference_publications'].split('DOI:')
                    for ref in refs:
                        print("bibref: ",ref.lstrip())

                if ";" in record['reference_publications']:
                    refs = record['reference_publications'].split(';')
                    for ref in refs:
                        print("bibref: ", ref.lstrip())

            if "body_system_or_organ_class" in record.keys():
                if ": " in record['body_system_or_organ_class']:
                    organs = record['body_system_or_organ_class'].split(': ')
                    for organ in organs:
                        print(organ.lstrip())
                elif ";" in record['body_system_or_organ_class']:
                    organs = record['body_system_or_organ_class'].split(';')
                    for organ in organs:
                        print("organ: ",organ.lstrip())

            if "samples_type" in record.keys():
                # print(record["samples_type"])
                samples = record['samples_type']
                for sample in samples:
                        print("sample: ", sample.lstrip())

            if "indication" in record.keys():
                # print(record["samples_type"])
                disease = record['indication']
                # for disease in diseases:
                print("disease: ", disease.lstrip())