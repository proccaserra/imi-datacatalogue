#!/usr/bin/env python3

from ccmm.dats.datsobj import DatsObj
import ccmm.dats.util as util
import logging
import sys

# Produce a DATS Material for a single subject/donor.

def get_subject_dats_material(cache, p_subject, gh_subject, var_lookup):
    subj_id = p_subject['SUBJID']['mapped_value']

    # retrieve id reference for the Identifier of the DATS Dimension for the "all subjects" consent group version of the variable
    def get_var_id(name):
        return var_lookup[name]['dim'].get("identifier").getIdRef()

    # human experimental subject/patient
    subject_sex = DatsObj("Dimension", [
            ("name", util.get_value_annotation("Gender", cache)),
            ("description", "Gender of the subject"),
            ("identifier", get_var_id("SEX")),
            ("values", [ p_subject['SEX']['mapped_value'] ])
            ])

    subject_age = DatsObj("Dimension", [
            ("name", util.get_value_annotation("Age range", cache)),
            ("description", "Age range of the subject"),
            ("identifier", get_var_id("AGE")),
            ("values", [ p_subject['AGE']['mapped_value'] ])
            ])

    subject_hardy_scale = DatsObj("Dimension", [
            ("name", util.get_value_annotation("Hardy scale", cache)),
            ("description", "Hardy scale death classification for the subject"),
            ("identifier", get_var_id("DTHHRDY")),
            ("values", [ p_subject['DTHHRDY']['mapped_value'] ])
            ])

    subject_characteristics = [
        subject_sex,
        subject_age,
        subject_hardy_scale
        ]

    # use URI from GTEx id dump if present
    identifier = subj_id
    if gh_subject is not None:
        identifier = gh_subject['Destination URL']['raw_value']

    # human experimental subject/patient
    subject_material = DatsObj("Material", [
            ("name", subj_id),
            ("identifier", DatsObj("Identifier", [("identifier", identifier)] )),
            ("description", "GTEx subject " + subj_id),
            ("characteristics", subject_characteristics),
            ("taxonomy", [util.get_taxon_human(cache)]),
            ("roles", util.get_donor_roles(cache))
            ])

    # add to the cache
    subj_key = ":".join(["Material", subj_id])
    dats_subj = cache.get_obj_or_ref(subj_key, lambda: subject_material)

    return dats_subj

# Produce a dict of DATS subject/donor Materials, indexed by GTEx subject id.

def get_subjects_dats_materials(cache, p_subjects, gh_subjects, var_lookup):
    dats_subjects = {}

    for s in p_subjects:
        # subject phenotype info from GTEx Portal file
        p_subject = p_subjects[s]
        # subject info from GTEx GitHub id dump
        gh_subject = gh_subjects[s]
        subj_id = p_subject['SUBJID']['mapped_value']
        subj_material = get_subject_dats_material(cache, p_subject, gh_subject, var_lookup)
        dats_subjects[subj_id] = subj_material
    
    return dats_subjects

