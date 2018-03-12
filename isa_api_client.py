import glob
import os
import logging
import time

import errno
from isatools.convert import isatab2json
from isatools.isatab import load, dump
import json
from isatools.model import *
from isatools.isajson import ISAJSONEncoder

logger = logging.getLogger(__name__)


class IsaApiClient:

    def __init__(self):
        self.inv_filename = "i_Investigation.txt"

        return

    def _write_study_json(self, inv_obj, std_path, skip_dump_tables=True):

        # Using the new feature in isatools, implemented from issue #185
        # https://github.com/ISA-tools/isa-api/issues/185
        # isatools.isatab.dump() writes out the ISA as a string representation of the ISA-Tab,
        # skipping writing tables, i.e. only i_investigation.txt
        logger.info("Writing %s to %s", self.inv_filename, std_path)
        try:
            os.makedirs(std_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        inv = dump(inv_obj, std_path, i_file_name=self.inv_filename, skip_dump_tables=skip_dump_tables)

        return inv

    def new_study(self, std_title, std_description, mtspc_obj, output_dir, persist=False):
        print("Creating ISA-Tab investigation file.")

        # investigation file
        investigation = Investigation(filename="i_investigation.txt")
        investigation.title = ""
        investigation.description = ""
        # investigation.submission_date = time.strftime("%d-%m-%Y")
        # investigation.public_release_date = time.strftime("%d-%m-%Y")

        submittedby = mtspc_obj[0]['Submitted_By']
        ppal_inv = submittedby['Principal_Investigator']
        submitter = submittedby['Submitter']

        ct_ppal_inv = Person(first_name=ppal_inv['First_Name'],
                             last_name=ppal_inv['Surname'],
                             affiliation=submittedby['Institution'],
                             email=ppal_inv['Email'],
                             roles=[OntologyAnnotation(term='submitter'),
                                    OntologyAnnotation(term='principal investigator role')])
        ct_submitter = Person(first_name=submitter['First_Name'],
                              last_name=submitter['Surname'],
                              affiliation=submittedby['Institution'],
                              email=submitter['Email'],
                              roles=[OntologyAnnotation(term='submitter')])

        # study file
        study = Study(filename="s_study.txt")
        # study.identifier = "s1"
        study.title = std_title
        study.description = std_description
        study.submission_date = time.strftime("%d-%m-%Y")
        study.public_release_date = time.strftime("%d-%m-%Y")

        # If different submitters, PI becomes submitter
        if ppal_inv['Surname'] != submitter['Surname'] \
                and ppal_inv['First_Name'] != submitter['First_Name']:
            investigation.contacts.append(ct_ppal_inv)
            investigation.contacts.append(ct_submitter)
            study.contacts.append(ct_ppal_inv)
            study.contacts.append(ct_submitter)
        else:
            investigation.contacts.append(ct_ppal_inv)
            investigation.contacts.append(ct_submitter)
            study.contacts.append(ct_ppal_inv)
            study.contacts.append(ct_submitter)

        investigation.studies.append(study)

        # CONNECT TO METASPACE SERVICES
        database = "HMDB"
        fdr = 0.1
        from sm_annotation_utils import sm_annotation_utils
        sm = sm_annotation_utils.SMInstance()  # connect to the main metaspace service
        db = sm._moldb_client.getDatabase(database)  # connect to the molecular database service

        # assay file
        assay = Assay(filename="a_assay.txt")

        for sample in mtspc_obj:
            metaspace_options = sample['metaspace_options']
            ds_name = metaspace_options['Dataset_Name']
            ds = sm.dataset(name=ds_name)

            assay.samples.append(sample)


        # extraction_protocol = Protocol(name='extraction', protocol_type=OntologyAnnotation(term="material extraction"))
        # study.protocols.append(extraction_protocol)
        # sequencing_protocol = Protocol(name='sequencing', protocol_type=OntologyAnnotation(term="material sequencing"))
        # study.protocols.append(sequencing_protocol)
        study.assays.append(assay)

        if persist:
            self._write_study_json(investigation, output_dir, skip_dump_tables=False)

        return investigation


