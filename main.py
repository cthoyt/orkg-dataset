# -*- coding: utf-8 -*-

"""Download and organize the ORKG for use in knowledge graph embedding models."""

import os
import pickle
from urllib.request import urlretrieve

import click
import rdflib
from tqdm import tqdm

#: URL of the ORKG n-triples dump
URL = 'https://orkg.org/orkg/api/rdf/dump'

HERE = os.path.abspath(os.path.dirname(__file__))
RAW_PATH = os.path.join(HERE, 'raw.nt')
CLEANED_PATH = os.path.join(HERE, 'processed.nt')
CLEANED_TTL_PATH = os.path.join(HERE, 'processed.ttl')
CLEANED_PICKLE_PATH = os.path.join(HERE, 'processed.pkl')
EXPORT_TSV_PATH = os.path.join(HERE, 'processed.tsv')

namespaces = {
    'orkg.class': rdflib.Namespace('http://orkg.org/orkg/class/'),
    'orkg.predicate': rdflib.Namespace('http://orkg.org/orkg/predicate/'),
    'orkg.resource': rdflib.Namespace('http://orkg.org/orkg/resource/'),
    'owl': rdflib.Namespace('http://www.w3.org/2002/07/owl#'),
}


@click.command()
def main():
    # TODO extract version information

    # Download the data if it does not exist
    if not os.path.exists(RAW_PATH):
        print(f'downloading {URL} to {RAW_PATH}')
        urlretrieve(URL, RAW_PATH)

    # There are some malformed lines that contain ``null`` instead of
    # a proper URL. This code removes those lines.
    if not os.path.exists(CLEANED_PATH):
        with open(RAW_PATH) as infile, open(CLEANED_PATH, 'w') as outfile:
            for line in infile:
                if '<null>' not in line:
                    print(line.strip(), file=outfile)

    if os.path.exists(CLEANED_PICKLE_PATH):
        with open(CLEANED_PICKLE_PATH, 'rb') as file:
            graph = pickle.load(file)
    else:
        graph = rdflib.Graph()

        # Sit tight, this takes ~5 minutes or so
        graph.parse(CLEANED_PATH, format='nt')

        # output
        with open(CLEANED_PICKLE_PATH, 'wb') as file:
            pickle.dump(graph, file, protocol=pickle.HIGHEST_PROTOCOL)

        # output as turtle
        graph.serialize(CLEANED_TTL_PATH, format='turtle')

    # Output a constrained version
    with open(EXPORT_TSV_PATH, 'w') as file:
        for s, p, o in tqdm(graph, unit_scale=True):
            if isinstance(o, rdflib.term.Literal):
                continue
            print(s, p, o, file=file, sep='\t')


if __name__ == '__main__':
    main()
