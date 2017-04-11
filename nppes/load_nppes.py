import csv
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import time
import os
import argparse
import zipfile
import logging
from functools import reduce
import io

default_elastic_url = 'http://winevlnstst79.hosted.appsevolenthealth.com:8100'

# move this to somewhere that's easy to change
nppes_mapping = {
    'npi': 'NPI',
    'entity_type_code': 'Entity Type Code',
    'first_name': 'Provider First Name',
    'middle_name': 'Provider Middle Name',
    'last_name': 'Provider Last Name (Legal Name)',
    'name_prefix': 'Provider Name Prefix Text',
    'name_suffix': 'Provider Name Suffix Text',
    'other_last_name': 'Provider Other Last Name',
    'other_first_name': 'Provider Other First Name',
    'other_middle_name': 'Provider Other Middle Name',
    'mailing_address_1': 'Provider First Line Business Mailing Address',
    'mailing_address_2': 'Provider Second Line Business Mailing Address',
    'city': 'Provider Business Mailing Address City Name',
    'state': 'Provider Business Mailing Address State Name',
    'postal_code': 'Provider Business Mailing Address Postal Code',
    'telephone_number': 'Provider Business Mailing Address Telephone Number',
    'fax_number': 'Provider Business Mailing Address Fax Number',
    "authorized_telephone_number": 'Authorized Official Telephone Number',
    'practice_location_address_1': 'Provider First Line Business Practice Location Address',
    'practice_location_address_2': 'Provider Second Line Business Practice Location Address',
    'practice_location_city': 'Provider Business Practice Location Address City Name',
    'practice_location_state': 'Provider Business Practice Location Address State Name',
    'practice_location_postal_code': 'Provider Business Practice Location Address Postal Code',
    'practice_location_telephone_number': 'Provider Business Practice Location Address Telephone Number',
    'practice_location_fax_number': 'Provider Business Practice Location Address Fax Number',
    'credentials': 'Provider Credential Text',
    'other_credentials': 'Provider Other Credential Text',
    'orginization_name': 'Provider Organization Name (Legal Business Name)'
}

def load_taxonomy(nucc):
    nucc_dict = {}
    with open(nucc) as nucc_file:
        nucc_reader = csv.DictReader(nucc_file)
        for row in nucc_reader:
            code = row['Code']
            classification = row['Classification']
            specialization = row['Specialization']
            if code and classification:
                nucc_dict[code] = classification + " " + specialization
    return nucc_dict


def extract_provider(row, nucc_dict):
    # creates the Lucene "document" to define this provider
    # assumes this is a valid provider
    provider_document = {}

    for (key, value) in nppes_mapping.items():
        provider_document[key] = row.get(value, '')
        provider_document['specialities'] = [nucc_dict.get(row['Healthcare Provider Taxonomy Code_1'], ''),	nucc_dict.get(row['Healthcare Provider Taxonomy Code_2'], ''),	nucc_dict.get(row['Healthcare Provider Taxonomy Code_3'], '')]
    return provider_document

def convert_to_json(provider_doc):
    # some kind of funky problem with non-ascii strings here
    # trap and reject any records that aren't full ASCII.
    # fix me!
    try:
        j = json.dumps(provider_doc, ensure_ascii=True)
    except Exception:
        j = None
    return j

# create a python iterator for ES's bulk load function


def iter_nppes_data(nppes_file, nucc_dict, convert_to_json):
    # extract directly from the zip file
    zip_file_instance = zipfile.ZipFile(nppes_file, "r", allowZip64=True)
    for zip_info in zip_file_instance.infolist():
        # hack - the name can change, so just use the huge CSV. That's
        # the one
        if zip_info.file_size > 4000000000:
            print("found NPI CSV file = ", zip_info.filename)
            # rU = universal newline support!
            #content = zip_file_instance.open(zip_info, 'rU')
            content = zip_file_instance.open(zip_info, 'r')
            content = io.TextIOWrapper(content)
            reader = csv.DictReader(content)
            for row in reader:
                provider_doc = extract_provider(row, nucc_dict)
                json = convert_to_json(provider_doc)
                if json:
					# action instructs the bulk loader how to handle this
					# record
                    action = {
                        "_index": "nppes3",
                        "_type": "provider",
                        "_id": provider_doc['npi'],
                        "_source": json
                    }
                    yield action


# main code starts here
def loadFiles(nppes_file, nucc_file):
    nucc_dict = load_taxonomy(nucc_file)

    elastic = Elasticsearch([
        elasticUrl
    ])

    start = time.time()
    #timeString = time.strftime("%Y%m%d%H%M%S", start)
    logging.warning("start at", str(start))

    # invoke ES bulk loader using the iterator
    helpers.bulk(elastic, iter_nppes_data(nppes_file, nucc_dict, convert_to_json))

    logging.warning("total time - seconds", time.time() - start)

elasticUrl = input("enter elastic url (defaults to dev url if not provided):")
if not elasticUrl:
    elasticUrl = default_elastic_url

#todo: run an automated script to download the file and execute the weekly ingest
#first time ingest the full file, use zip file
nppes_file = 'C:/Users/gsrinivasan/Documents/elastic/nppes/data/NPPES_Data_Dissemination_March_2017.zip'
#taxonomy file from http://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57
nucc_file = 'C:/Users/gsrinivasan/Documents/elastic/nppes/data/nucc_taxonomy_170.csv'
loadFiles(nppes_file, nucc_file)
