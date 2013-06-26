#!/usr/bin/env python

"""
DESCRIPTION:
    Take census XML.  Use API to download & generate...
    - 1 CSV per concept
      - containing one column per concept variable
        - named as "{CONCEPTID}.csv" (e.g. "P12.csv")
    - 'metadata.csv'
      - col1: concept name
      - col2: variable name
      - col3: variable text
"""

"""
This section not in docstring so skipped in help

-----

EXIT STATUS

    TODO: List exit codes
    1 is definitely a failure.
    0 might be success
-----

AUTHOR

    Jason Brown <JBrown@edac.unm.edu>
-----

LICENSE

Copyright (c) 2013, Jason Brown
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are those
of the authors and should not be interpreted as representing official policies,
either expressed or implied, of the FreeBSD Project.


2-clause "Simplified BSD License".

-----

VERSION

    $Id: $
-----
"""

# pylint -- name convention
#pylint: disable-msg=C0103

# CORE
import argparse
import codecs
import csv
import itertools
import logging
import os
import random
import signal  # irksome python unix pipe issue
import simplejson
import sys
import time
import traceback
import urllib2

from os.path import join as opj

# NON CORE
from lxml import etree

# These are logging _functions_
INFO = logging.info
WARN = logging.warning
DEBUG = logging.debug
ERR = logging.error


#################################################
#   CONFIGURATION
#################################################

LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'

CENSUS_API_KEY = '6bc84b8daed00a678df401d13546c3696bdfdfbb'
SF1_URL = 'http://api.census.gov/data/2010/sf1'
VARS_PER_QUERY = 15  # Listed max is 50 at which they err

# To be respectful to the server, we do not hose them between requests
POLITE_SLEEP = (1, 3)   # min, max seconds


#----------------------------------------------------------------------
def getURL(url):
    """
    wget url
    """

    DEBUG('\tDownloading: ' + url)
    r = urllib2.urlopen(url)

    if r.code in [200]:
        return r.read()  # JSON
    elif r.code in [204]:
        msg = "No Content at (HTTP 204): " + url
        WARN(msg)
        return "[[]]"  # No data for this q
    elif r.code in [400, 500]:
        msg = url + "returned HTTP:" + r.code
        WARN(msg)
        raise Exception(msg)
    else:
        msg = url + "returned Unanticipated HTTP: " + r.code
        ERR(msg)
        raise Exception(msg)
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def updateProgress(percent=None,  # Static
                   dsMin=None, dsMax=None,  # Static, Both required if one sent
                   subMin=None, subMax=None,  # Both required if one sent
                   disable=None):  # Invocation setting
    """
    Print progress bar because this is slow app.

    Min's assumed as 0 index array elements, so we add 1

    Init me with updateProgress(0, 0, 1, 0, 1)
       (to look right)

    call with disable=True to stop printing
    """

    GRANULARITY = 20  # 5%
    up = updateProgress  # abbr

    # Assign statics
    if disable is not None:
        up.disable = disable

    # Shortcircuit.  It's set (not setting disable precludes assigning)
    if disable or getattr(up, 'disable', disable) is False:
        return

    # Rest of statics
    if percent is not None:
        up.percent = int(percent)

    if dsMin is not None:
        up.dsMin = dsMin + 1
        up.dsMax = dsMax

    if subMin is not None:
        up.subMin = subMin + 1
        up.subMax = subMax

    #'[####################] 999 % | CSV 999 of 999 | GET 999 of 999'
    print '\r[{0}] {1} % | CSV {2} of {3} | GET {4} of {5}'.format(
        ('#' * (up.percent / (100 / GRANULARITY))).ljust(GRANULARITY),  # 0
        str(up.percent).rjust(3, ' '),  # 1

        "%03d" % int(up.dsMin),  # 2
        "%03d" % int(up.dsMax),  # 3

        "%03d" % int(up.subMin),  # 4
        "%03d" % int(up.subMax)  # 5
    ),
    sys.stdout.flush()  # force write so progress shows
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def groupByNumber(n):
    """
    Generator for itertools.groupby, clusters by periods of size N
    """

    repeater = itertools.cycle(
        [0] * n + [1] * n
    )
    return lambda x: repeater.next()
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def censusResponse2Rows(response):
    """
    http://api.census.gov/data/2010/sf1?
    key=6bc84b8daed00a678df401d13546c3696bdfdfbb
    &get=H011E0001,H011E0002,H011E0003,H011E0004&for=county:*&in=state:35
    """

    data_l = simplejson.loads(response)
    headers = data_l[0]
    data = data_l[1:]

    toRet = {}
    for row in data:
        row_d = dict(zip(headers, row))
        thisRow = {
            'state': row_d['state'],
            'county': row_d['county'],
        }
        key = frozenset(thisRow.items())  # key on tuple notation
        toRet[key] = row_d  # duplication in attr makes easier later

    return toRet
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def getEtree(sf1):
    """
    sf1: filehandle (read) to census summary file 1 xml
    """
    tree = None
    sf1.seek(0)
    tree = etree.parse(sf1)  # Of course, argparse doesn't close the handle

    return tree
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def xquery(tree, q):
    return tree.xpath(q)
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def getAPIVariables(tree):
    return xquery(tree, '/apivariables/concept/variable/@name')
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def getConcepts(tree):
    return xquery(tree, '/apivariables/concept/@name')
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def buildOutputDirs(args):
    """
    Test & Create
    """

    rootFolder = args.OUTDIR
    dataDir = opj(rootFolder, 'data')

    # Good
    if os.path.isdir(rootFolder) and os.path.isdir(dataDir):
        return

    try:
        os.makedirs(dataDir)
    except OSError:
        # Problem
        raise IOError("%(F)s or %(F)s/data already exist" %
            {'F': rootFolder})


#----------------------------------------------------------------------


#----------------------------------------------------------------------
def buildBadMD(tree, writeTo):
    """
    Sorry to metadata a CSV, world
    """

    root = tree.getroot()

    header = [
        'conceptName',
        'varName',
        'varText'
    ]

    # This probably needs to be UTF8-ified too

    #conceptName, varName, varText
    with open(opj(writeTo, 'metadata.csv'), 'w') as md:
        dw = csv.DictWriter(
            md,
            header
        )
        dw.writeheader()
        for concept in root.getchildren():
            for variable in concept.getchildren():
                outD = {
                    'conceptName': concept.attrib['name'],
                    'varName': variable.attrib['name'],
                    'varText': variable.text,
                }
                dw.writerow(outD)
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def getVarsByConcept(concept, tree):
    """
    """
    concept = xquery(
        tree,
        "/apivariables/concept[@name='%s']" % concept
    )[0]
    toRet = []
    for variable in concept.getchildren():
        name = variable.attrib['name']
        toRet.append(name)
        DEBUG("\t" + name)

    return toRet
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def downloadCensusData(varlist):
    """
    Return dict [keyed by frozenset (state, county)] of dictionaries
      of [varlist] = values

      Retrieved from census
    """

    rows = {}  # k={'state':35, 'county':001}

    workingList = []
    #  Save a copy of the queries
    # 1 URL per VPQ variables fetched, API supports up to 50
    for junk, shorterList in itertools.groupby(
        varlist,
        groupByNumber(VARS_PER_QUERY)
    ):
        workingList.append(list(shorterList))

    for idx, cherryPick in enumerate(workingList):
        url = "%(PRE)s?key=%(API)s&get=%(CHERRIES)s&for=county:*&in=state:35"
        url = url % {
            'PRE': SF1_URL,
            'API': CENSUS_API_KEY,
            'CHERRIES': ','.join(cherryPick)  # blow up the xargs
        }
        INFO('\t' + url)

        # dict of dicts
        try:
            data = censusResponse2Rows(
                getURL(url)
            )
        except:
            # Seems we sometimes get some...error messages.  Let's move on.
            msg = "Unable to process: %s" % url
            ERR(msg)
            continue
            #raise Exception("Unable to process: %s" % url)
        finally:
            # I am a nice scraper
            time.sleep(random.uniform(
                POLITE_SLEEP[0],
                POLITE_SLEEP[1]
            ))

        updateProgress(
            subMin=idx,
            subMax=len(workingList),
        )

        for key, val in data.items():
            #  key is frozenset
            DEBUG('\t\tUpdating Row with: [%s]' % cherryPick)
            #  I wish python dicts were more functional/chaining
            if key in rows:
                rows[key].update(val)
            else:
                rows[key] = val

    return rows
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def buildCSV(concept, filename, tree):
    """
    query API, build our service call, dump data
    """
    DEBUG("buildCSV for concept: " + concept)
    DEBUG("buildCSV outfile: " + filename)
    varlist = getVarsByConcept(concept, tree)

    # dict of dict key by fips, we get back 'state' and 'county'
    # period
    rows = downloadCensusData(varlist)

    # We have built our list, sort it by state, county
    flatRows = rows.values()   # Toss the key, we have a copy in attr
    flatRows.sort(key=lambda x: int(x['state']))  # sort
    flatRows.sort(key=lambda x: int(x['county']))  # subsort

    with open(filename, 'w') as out:
        header = ['state', 'county'] + varlist
        out.write(codecs.BOM_UTF8)  # MS needs BOM

        # header needs encoding
        out.write(
            ','.join(header).encode('utf8')
        )
        out.write('\n')
        dw = csv.DictWriter(
            out,
            header
        )
        #dw.writeheader()
        #dw.writerows(flatRows)
        for row in flatRows:
            dw.writerow({
                k: v.encode('utf8') for k, v in row.items()
            })
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def buildCSVs(tree, args):
    """
    Get list of concepts, make csv
    """

    concept_l = zip(
        getConcepts(tree),  # long name
        [i.split('.')[0] for i in getConcepts(tree)]  # file name
    )

    pairedDown = []

    for idx, (concept, shortName) in enumerate(concept_l):
        # Preserve original indexes
        if not args.conceptIDs or (idx + 1) in args.conceptIDs:
            pairedDown.append((idx, (concept, shortName)))

    #for idx, (concept, shortName) in enumerate(concept_l):
    for idx, (concept, shortName) in pairedDown:
        updateProgress(
            #percent=(idx / float(len(concept_l))) * 100,
            percent=(idx / float(len(pairedDown))) * 100,
            dsMin=idx,
            #dsMax=len(concept_l)
            dsMax=len(pairedDown)
        )
        buildCSV(
            concept,
            opj(args.OUTDIR, 'data', shortName + '.csv'),
            tree
        )
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def setLogger(args):
    """
    """

    logLevel = None
    if args.verbose is None:
        logLevel = logging.ERROR
    elif args.verbose is 1:
        logLevel = logging.WARN
    elif args.verbose is 2:
        logLevel = logging.INFO
    elif args.verbose >= 3:
        logLevel = logging.DEBUG
    else:  # Gratuitously defensive.  Should not happen.
        logLevel = logging.ERROR

    # Override logger with info level
    logging.basicConfig(level=logLevel, format=LOG_FORMAT)
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def mainConcepts(args):
    """
    print concept list function

    args: argparse.parse_args NS
    """

    tree = getEtree(args.SF1)

    concept_l = concept_l = zip(
        getConcepts(tree),  # long name
        [i.split('.')[0] for i in getConcepts(tree)]  # file name
    )

    for head in [
        ["ID #", "Concept", "Census Description"],
        ["-" * 5, "-" * 15, "-" * 75]
    ]:
        print "{0} | {1} | {2} ".format(
            head[0].center(5),
            head[1].center(15),
            head[2]
        )

    # Cannot use enumerate here...buggy list

    #index, filename, long concept
    #   5     15       rest
    for idx, (concept, shortName) in enumerate(concept_l):
        # If it isn't filtered, or if it is filtered and is in our list
        if not args.conceptIDs or (idx + 1) in args.conceptIDs:
            print "{0} | {1} | {2} ".format(
                str(idx + 1).rjust(5),
                shortName.rjust(15),
                concept
            )
#----------------------------------------------------------------------


##############################################################################
def main(args):
    """
    args: argparse.parse_args NS
    """

    setLogger(args)

    print ''  # CRLF for it to write to
    updateProgress(disable=args.progress)
    updateProgress(0, 0, 1, 0, 1)  # Initialize

    buildOutputDirs(args)

    tree = getEtree(args.SF1)
    buildBadMD(tree, args.OUTDIR)  # Write csv (easy to inspect, not really MD though)
    INFO('Metadata CSV generated')
    buildCSVs(tree, args)  # write lots of CSV
    updateProgress(100)  # Initialize
    INFO('Finished')

##############################################################################


##############################################################################
if __name__ == '__main__':

    signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # issues 1652
    try:
        start_time = time.time()

        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=__doc__.strip()
        )

        parser.add_argument(
            '-v', '--verbose', action='count', default=False,
            help='Verbose Output (stacks)')

        parser.add_argument(
            '-p', '--progress', action='store_true', default=False,
            help='Show Progress Bar (download only)')

        parser.add_argument(
            '-l', '--list', action='store_true', default=False,
            dest='listConcepts', help='Print List of Available concepts')

        parser.add_argument('conceptIDs', type=int,
            nargs='*', help="The concept ID #'s to fetch")

        parser.add_argument('-x', '--xml', type=argparse.FileType('r'),
            nargs='?', default='sf1.xml', dest='SF1',
            help='US Census Summary File 1 (default: sf1.xml)')

        parser.add_argument('-o', '--outdir', default='output',
            nargs='?', dest='OUTDIR',
            help='Output folder for files (default: outdir)')

        parsed = parser.parse_args()

        if parsed.listConcepts:
            mainConcepts(parsed)
        else:
            #  Soon we need an argument to build just 1 concept file
            main(parsed)

        if parsed.verbose > 0:
            INFO(time.asctime())
            INFO('TOTAL TIME IN SECONDS: %s' % (time.time() - start_time))

        sys.exit(os.EX_OK)
    except KeyboardInterrupt, e:  # Ctrl-C
        raise e
    except SystemExit, e:  # sys.exit()
        raise e
    except Exception, e:
        print 'ERROR, UNEXPECTED EXCEPTION'
        print str(e)
        traceback.print_exc()
        sys.exit(1)

##############################################################################
