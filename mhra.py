"""
Fetch data from the MHRA to link against Drugs
"""
import collections
import itertools
import sys
import os

import argparse
import ffs
import gevent
from gevent import monkey
from lxml import html
import requests

monkey.patch_all()

datadir = ffs.Path.newdir()

LICENCE_URL = "http://www.mhra.gov.uk/Howweregulate/Medicines/Licensingofmedicines/\
Informationforlicenceapplicants/Otherusefulservicesandinformation/\
Listsofapprovedproducts/Marketingauthorisations/index.htm"

pagebreak = """* POM = Prescription only medicine
P = Pharmacy only medicine
GSL = General Sale List"""

class LicenceParser(object):
    """
    Parse a text version of MHRA licence approval PDFs
    """

    section_names = [
        'pl_number',
        'grant_date',
        'ma_holder',
        'names',
        'ingredient',
        'quantity',
        'legal_status_units'
        ]

    sections = itertools.cycle(section_names)

    headings = [
        'PL Number', 'Units', 'Legal Status',
        'Quantity', 'Active Ingredient', 'Licensed Name(s)',
        'MA Holder',
        'Grant DATE'
        ]

    def __init__(self, contents, fname):
        self._contents = contents
        self._fname = fname
        self.drugs = collections.defaultdict(list)
        self.section = None

    def parse(self):

        pages = self._contents.split(pagebreak)

        self.section = self.sections.next()

        for page in pages:


            mypage = page
            frist = True

            lines = [f for f in mypage.split("\n")
                     if not f.startswith("Marketing authorisations granted in")
                     and f not in self.headings]
            lines = [l for i, l in enumerate(lines) if not (l == '' and lines[i-1] == '')]


            for line in lines:
                if line == '':
                    # Section break
                    self.section = self.sections.next()
                    continue

                elif self.section:
                    try:
                        getattr(self, 'do_{0}'.format(self.section))(line)
                    except AttributeError:
                        import pdb; pdb.set_trace()
                        print line, self.key
                else:
                    print 'No section&& not blank'

                    import pdb; pdb.set_trace()
                    print line

        return self.write()

    def do_simple(self, line):
        """
        Parse a sigle line by just adding it to the drugs dict

        Arguments:
        - `line`: str

        Return: None
        Exceptions: None
        """
        self.drugs[self.section].append(line)
        return

    def do_legal_status_units(self, line):
        """
        Some formatting issues mean that the units and legal status come
        through together. Herein, we split them back out.

        Arguments:
        - `line`: str

        Return: None
        Exceptions: None
        """
        try:

            try:
                units, status = line.split()
            except ValueError:
                # Pfft. Formatting. Hack it for now & see how messy it is
                if line == 'MICROGRAMMESPOM':
                    units, status = 'MICROGRAMMES', 'POM'

            self.drugs['units'].append(units)
            self.drugs['status'].append(status)



        except Exception as err:

            import pdb; pdb.set_trace()
            print line

        return

    do_pl_number = do_simple
    do_grant_date = do_simple
    do_ma_holder = do_simple
    do_names = do_simple
    do_ingredient = do_simple
    do_quantity = do_simple

    def write(self):
        """
        Assuming we've parsed a PDF, let's write it out to a
        CSV file

        Return: filename
        Exceptions: None
        """
        filename = datadir / 'licencsv' / str(self._fname).replace('pdf', 'csv')

        mat = []
        for f in ['pl_number',
                  'grant_date',
                  'ma_holder',
                  'names',
                  'ingredient',
                  'quantity',
                  'status',
                  'units'
                  ]:
            mat.append(self.drugs[f])

        csvready = zip(*mat)

        print "Writing csv at ", filename

        with filename.csv() as csv:
            csv.writerow(self.section_names)
            csv.writerows(csvready)


        return filename

def parse_licence(fname):
    """
    Given the FNAME of our PDF, parse it and write as a csv

    Arguments:
    - `fname`: Path

    Return: None
    Exceptions: None
    """
    parser = LicenceParser(fname.contents, fname[-1])
    fname = parser.parse()
    return fname

def fetch_pdf_batch(pdfs):
    """
    Given a list of urls at which we might find PDFs, fetch these
    and save them to our datadir.

    Arguments:
    - `pdfs`: [str,]

    Return: None
    Exceptions: None
    """
    jobs = [gevent.spawn(requests.get, u) for u in pdfs]
    gevent.joinall(jobs)

    files = []
    for greenlet in jobs:
        response = greenlet.get()
        ourfile = datadir / 'licences' / os.path.basename(response.url)
        ourfile << response.content
        files.append(ourfile)

    print 'Parsing PDF batch'
    jobs = [gevent.spawn(parse_licence, f) for f in files]
    gevent.joinall(jobs)
    return

def fetch_pdfs(visited, pdfs, url):
    """
    Given a set of visited urls, fetched pdf files, and
    the url we are currently interested in, let's download
    all the authorisation pdfs to a local directory.


    Arguments:
    - `visited`: set
    - `pdfs`:set
    - `url`: str

    Return: None
    Exceptions: None
    """
    markup = html.document_fromstring(requests.get(url).content)
    pdflinx = set([a.get('href') for a in markup.cssselect('li h3 a')])
    unseen = pdflinx.difference(pdfs)
    if unseen:
        print "New PDFs", unseen
        pdfs.update(unseen)
        fetch_pdf_batch(unseen)
    return

def fetch_licences():
    """
    Fetch the licence approvals to disk

    Return: None
    Exceptions:None
    """
    datadir.mkdir('licencsv')

    visited = set()
    pdfs = set()
    print "Fetxhing licences at", LICENCE_URL
    fetch_pdfs(visited, pdfs, LICENCE_URL)
    return

def main(args):
    """
    Entrypoint when run as a script

    Arguments:
    - `args`: argparse Arglist

    Return: int
    Exceptions: None
    """
    fetch_licences()
    print 'datadir was', datadir
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fetch and visualise website sitemaps")
    args = parser.parse_args()

    sys.exit(main(args))
