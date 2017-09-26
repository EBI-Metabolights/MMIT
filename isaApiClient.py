import glob
import os
import logging
import time
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

    def _write_study_json(self, inv_obj, std_path):

        # Using the new feature in isatools, implemented from issue #185
        # https://github.com/ISA-tools/isa-api/issues/185
        # isatools.isatab.dump() writes out the ISA as a string representation of the ISA-Tab,
        # skipping writing tables, i.e. only i_investigation.txt
        logger.info("Writing %s to %s", self.inv_filename, std_path)
        inv = dump(inv_obj, std_path, i_file_name=self.inv_filename, skip_dump_tables=True)

        return inv

    def create_new_study(self, title, description, sub_date, pub_rel_date):
        """
        Create a new MTBLS Study from a METASPACE metadata JSON file

        :return: an ISA-JSON representation of the Study
        """

        # investigation file
        investigation = Investigation(filename="i_investigation.txt")
        investigation.title = title
        investigation.description = description
        investigation.submission_date = sub_date
        investigation.public_release_date = pub_rel_date

        # study file
        study = Study(filename="s_study.txt")
        study.identifier = "s1"
        study.title = title
        study.description = description
        study.submission_date = sub_date
        study.public_release_date = pub_rel_date
        investigation.studies.append(study)

        # assay file
        assay = Assay(filename="a_assay.txt")
        extraction_protocol = Protocol(name='extraction', protocol_type=OntologyAnnotation(term="material extraction"))
        study.protocols.append(extraction_protocol)
        sequencing_protocol = Protocol(name='sequencing', protocol_type=OntologyAnnotation(term="material sequencing"))
        study.protocols.append(sequencing_protocol)
        study.assays.append(assay)

        return json.dumps(investigation, cls=ISAJSONEncoder, sort_keys=True, indent=4, separators=(',', ': '))

