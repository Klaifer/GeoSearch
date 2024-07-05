import json
import os
import sys
import argparse
import stat
import urllib.request
import zipfile
from bs4 import BeautifulSoup
import whoosh.index as wi
import whoosh.fields as wf
import whoosh.qparser as wp
from tqdm import tqdm
from datetime import datetime
import whoosh.query as wq
import warnings


class FuzzyTerm2(wq.FuzzyTerm):
    def __init__(self, fieldname, text, boost=1.0, maxdist=2, prefixlength=1, constantscore=True):
        super(FuzzyTerm2, self).__init__(fieldname, text, boost, maxdist, prefixlength, constantscore)


class GeoSearch:
    DEFAULT_DATA = "https://download.geonames.org/export/dump/allCountries.zip"
    DEFAULT_CODES = "http://www.geonames.org/export/codes.html"

    schema = wf.Schema(
        geonameid=wf.ID(stored=True, unique=True),
        name=wf.TEXT(stored=True),
        asciiname=wf.TEXT(stored=True),
        alternatenames=wf.TEXT(stored=True),
        latitude=wf.NUMERIC(stored=True, numtype=float),
        longitude=wf.NUMERIC(stored=True, numtype=float),
        feature_class=wf.STORED(),
        feature_code=wf.TEXT(stored=True),
        country_code=wf.TEXT(stored=True),
        cc2=wf.STORED(),
        admin1_code=wf.TEXT(stored=True),
        admin2_code=wf.TEXT(stored=True),
        admin3_code=wf.TEXT(stored=True),
        admin4_code=wf.TEXT(stored=True),
        population=wf.STORED(),
        elevation=wf.STORED(),
        dem=wf.STORED(),
        timezone=wf.STORED(),
        modification_date=wf.STORED()
    )

    def __init__(self, download_path=None, server_data_url=None, server_codes_url=None):
        """
        Geosearch is an offline lib to query geonames

        :param server_data_url: Change the default geonames url (http://www.geonames.org/allCountries.zip)
        :param server_codes_url: Change the dafault codes page url (http://www.geonames.org/export/codes.html)
        :param download_path: Change de default store path
        """
        self._data_url = server_data_url or self.DEFAULT_DATA
        self._codes_url = server_codes_url or self.DEFAULT_CODES
        self._download_dir = download_path
        self.place_codes = None

        if not download_path:
            self._download_dir = GeoSearch._get_download_path()
        index_dir = os.path.join(self._download_dir, "indexdir")

        try:
            self.whooidx = wi.open_dir(index_dir)
        except wi.EmptyIndexError:
            raise RuntimeError(
                "The database was not found. Execute download() first."
            )

    def find(self, querystr, limit=10, maxdist=2, hierarchy=True, expand_codes=True):
        """
        Search by place name

        :param querystr: Place name
        :param limit: Maximum number of results
        :param maxdist: Includes a little variation in the search query's results. Greater distances are slower.
        :param hierarchy: True to search for hierarchically superior regions
        :param expand_codes: True for full description of the class and code.
        :return: A list of places
        """
        if maxdist > 2:
            raise ValueError("Maxdist parameter must be less than 3")

        found = []
        with self.whooidx.searcher() as searcher:
            levels = ['exact']
            if maxdist != 0:
                levels.append('single')
            if maxdist == 2:
                levels.append('double')

            for level in levels:
                if level == 'exact':
                    q = wp.MultifieldParser(["name", "asciiname", "alternatenames"], self.whooidx.schema)
                elif level == 'single':
                    q = wp.MultifieldParser(["name", "asciiname", "alternatenames"], self.whooidx.schema,
                                            termclass=wq.FuzzyTerm)
                elif level == 'double':
                    q = wp.MultifieldParser(["name", "asciiname", "alternatenames"], self.whooidx.schema,
                                            termclass=FuzzyTerm2)

                q = q.parse(querystr)
                res = searcher.search(q, limit=limit, scored=True)
                found = self._response(res, searcher, hierarchy=hierarchy, expand_codes=expand_codes)

                if found:
                    break

        return found

    def findpos(self, lat, lon, limit=10, range=0.001, hierarchy=True, expand_codes=True):
        """
        Search by geographic coordinates

        :param lat: Latitude
        :param lon: Longitude
        :param limit: Maximum number of results
        :param range: Margin of the square with the given point in the center.
        :param hierarchy: True to search for hierarchically superior regions
        :param expand_codes: True for full description of the class and code.
        :return: A list of places
        """
        with self.whooidx.searcher() as searcher:
            q = wq.And([
                wq.NumericRange("latitude", lat - range, lat + range),
                wq.NumericRange("longitude", lon - range, lon + range)
            ])
            res = searcher.search(q, limit=limit, scored=True)
            found = self._response(res, searcher, hierarchy=hierarchy, expand_codes=expand_codes)

        return found

    def _response(self, result, searcher, hierarchy, expand_codes):
        found = []
        if result:
            for res in result:
                atb = res.fields()

                if expand_codes:
                    self._add_description(atb)

                if hierarchy:
                    self._add_hierarchy(atb, searcher)

                found.append({
                    'score': res.score,
                    'place': atb
                })

        return found

    @staticmethod
    def download(download_path=None, data_url=None, codes_url=None, overwrite=True):
        """
        Update geonames data.

        The file in data_url is dowloaded and indexed on download_path. Changing data_url its possible to select a
            single country. It´s possible to use overwrite=False to join data from multiple countries.

        This data use codes to describe places, like lake and peak. Using codes_url its possible to include full
            place descriptions

        :param download_path: Change de default store path
        :param data_url:    Change the default geonames url (http://www.geonames.org/allCountries.zip)
        :param codes_url:   Change the dafault codes page url (http://www.geonames.org/export/codes.html)
        :param overwrite:   True to create a new dataset(current data will be REMOVED). False to append data to the
                                current dataset (slow)
        :return:
        """
        if not download_path:
            download_path = GeoSearch._get_download_path()
        if not data_url:
            data_url = GeoSearch.DEFAULT_DATA
        if not codes_url:
            codes_url = GeoSearch.DEFAULT_CODES

        geonames_path = os.path.join(download_path, "geonames")
        os.makedirs(geonames_path, exist_ok=True)

        try:
            GeoSearch._build_feature_codes(os.path.join(download_path, "placecodes.json"), codes_url)
        except AttributeError:
            warnings.warn(
                "Unable to update geocodes. The results will not include the full description of location codes.")

        geonames_zipfile = os.path.join(download_path, "geonames.zip")
        urllib.request.urlretrieve(data_url, geonames_zipfile)
        with zipfile.ZipFile(geonames_zipfile, 'r') as zip_ref:
            zip_ref.extractall(geonames_path)
            content_files = zip_ref.namelist()

        index_dir = os.path.join(download_path, "indexdir")
        append = overwrite

        for filein in content_files:
            try:
                fullpath = os.path.join(geonames_path, filein)
                GeoSearch._indexfile(fullpath, index_dir, filein, overwrite=overwrite, append=append)
                overwrite=False
                os.remove(fullpath)
            except Exception as e:
                pass

        os.remove(geonames_zipfile)

    def _add_description(self, atb):
        if not self.place_codes:
            fname = os.path.join(self._download_dir, "placecodes.json")
            with open(fname, "r", encoding="utf-8") as fin:
                self.place_codes = json.load(fin)

        classid = atb.get('feature_class', '')
        codeid = atb.get('feature_code', '')

        fclass = self.place_codes.get(classid, {'descr': None, 'codes': {}})
        fclass_descr = fclass['descr']
        fcode = fclass['codes'].get(codeid, {})
        fcode_short = fcode.get("short", "")
        fcode_full = fcode.get("full", "")
        atb['feature_class_descr'] = fclass_descr
        atb['feature_code_short'] = fcode_short
        atb['feature_code_full'] = fcode_full

    def _add_hierarchy(self, atb, searcher):
        country_code = atb["country_code"].lower()
        conditions = [wq.Term("country_code", country_code)]
        fnum = ["1", "2", "3", "4"]
        excludeatb = ["admin" + k + "_code" for k in fnum] + ['feature_code', 'feature_class', 'country_code', "cc2"]

        for l in fnum:
            admincode = "admin" + l + "_code"
            adminvalue = atb[admincode].lower()
            if not adminvalue:
                break

            conditions += [wq.Term(admincode, adminvalue)]
            query = wq.And(conditions + [wq.Term("feature_code", "adm" + l)])

            results = searcher.search(query)
            if len(results) == 1:
                other = results[0].fields()
                for key in excludeatb:
                    other.pop(key, None)
                atb['admin' + l] = other
            else:
                break

    @staticmethod
    def _indexfile(filein, indexdir, descr, overwrite=True, append=True):
        if overwrite or (not os.path.exists(indexdir)):
            os.makedirs(indexdir, exist_ok=True)
            whooidx = wi.create_in(indexdir, GeoSearch.schema)
        else:
            whooidx = wi.open_dir(indexdir)

        whoowriter = whooidx.writer()

        atbnames = ['geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude', 'feature_class',
                    'feature_code', 'country_code', 'cc2', 'admin1_code', 'admin2_code', 'admin3_code', 'admin4_code',
                    'population', 'elevation', 'dem', 'timezone', 'modification_date']

        num_lines = sum(1 for _ in open(filein, "r", encoding="utf-8"))
        with open(filein, "r", encoding="utf-8") as fin:
            for iline, line in enumerate(tqdm(fin, dynamic_ncols=True, total=num_lines, desc=descr)):
                attb = line.split("\t")
                assert len(attb) == len(atbnames)
                sample = {k: v for k, v in zip(atbnames, attb)}
                sample['latitude'] = float(sample['latitude'])
                sample['longitude'] = float(sample['longitude'])
                sample['population'] = int(sample['population']) if sample['population'] else None
                sample['elevation'] = int(sample['elevation']) if sample['elevation'] else None
                sample['dem'] = int(sample['dem']) if sample['dem'] else None
                sample['modification_date'] = datetime.strptime(
                    sample['modification_date'][:10], '%Y-%m-%d'
                ) if sample['modification_date'] else None

                if append:
                    whoowriter.add_document(**sample)
                else:
                    whoowriter.update_document(**sample)

                if iline % 1000000 == 999999:
                    whoowriter.commit(optimize=not append)
                    whoowriter = whooidx.writer()

            whoowriter.commit(optimize=not append)

    @staticmethod
    def _build_feature_codes(fileout, codes_url):
        page = urllib.request.urlopen(codes_url)
        content = page.read()
        soup = BeautifulSoup(content, 'lxml')
        table = soup.find('table', {'class': 'restable'})

        feature_code = {}
        for el in table.find_all("tr"):
            trclass = el.find_all("th")
            trcode = el.find_all("td")

            if trclass:
                text = trclass[0].getText()
                fclass, fcldesc = text.split(" ", 1)
            elif trcode:
                if len(trcode) == 3:
                    code = trcode[0].getText().strip()
                    short_descr = trcode[1].getText().strip()
                    full_descr = trcode[2].getText().strip()

                    curr = feature_code.get(fclass, {'descr': fcldesc, 'codes': {}})
                    codes = curr['codes']
                    codes[code] = {
                        'short': short_descr,
                        'full': full_descr
                    }

                    feature_code[fclass] = curr

        with open(fileout, "w", encoding="utf-8") as fout:
            fout.write(json.dumps(feature_code, indent=4))

    @staticmethod
    def _default_paths():
        """
        Search Path - Code borrowed from nltk
        :return:
        """

        path = []
        """A list of directories where the GeoSearch data package might reside.
           These directories will be checked in order when looking for a
           resource in the data package.  Note that this allows users to
           substitute in their own versions of resources, if they have them
           (e.g., in their home directory under ~/geosearch_data)."""

        # User-specified locations:
        _paths_from_env = os.environ.get("GEOSEARCH_DATA", "").split(os.pathsep)
        path += [d for d in _paths_from_env if d]
        if "APPENGINE_RUNTIME" not in os.environ and os.path.expanduser("~/") != "~/":
            path.append(os.path.expanduser("~/geosearch_data"))

        if sys.platform.startswith("win"):
            # Common locations on Windows:
            path += [
                os.path.join(sys.prefix, "geosearch_data"),
                os.path.join(sys.prefix, "share", "geosearch_data"),
                os.path.join(sys.prefix, "lib", "geosearch_data"),
                os.path.join(os.environ.get("APPDATA", "C:\\"), "geosearch_data"),
                r"C:\geosearch_data",
                r"D:\geosearch_data",
                r"E:\geosearch_data",
            ]
        else:
            # Common locations on UNIX & OS X:
            path += [
                os.path.join(sys.prefix, "geosearch_data"),
                os.path.join(sys.prefix, "share", "geosearch_data"),
                os.path.join(sys.prefix, "lib", "geosearch_data"),
                "/usr/share/geosearch_data",
                "/usr/local/share/geosearch_data",
                "/usr/lib/geosearch_data",
                "/usr/local/lib/geosearch_data",
            ]
        return path

    @staticmethod
    def _get_download_path():
        """
        Code borrowed from nltk (https://www.nltk.org/)

        Return the directory to which packages will be downloaded by
        default.  This value can be overridden using the constructor,
        or on a case-by-case basis using the ``download_dir`` argument when
        calling ``download()``.

        On Windows, the default download directory is
        ``PYTHONHOME/lib/geosearch``, where *PYTHONHOME* is the
        directory containing Python, e.g. ``C:\\Python25``.

        On all other platforms, the default directory is the first of
        the following which exists or which can be created with write
        permission: ``/usr/share/geosearch_data``, ``/usr/local/share/geosearch_data``,
        ``/usr/lib/geosearch_data``, ``/usr/local/lib/geosearch_data``, ``~/geosearch_data``
        """

        # Check if we are on GAE where we cannot write into filesystem.
        if "APPENGINE_RUNTIME" in os.environ:
            return

        # Check if we have sufficient permissions to install in a
        # variety of system-wide locations.
        for geosearchdir in GeoSearch._default_paths():
            if os.path.exists(geosearchdir) and GeoSearch._is_writable(geosearchdir):
                return geosearchdir

        # On Windows, use %APPDATA%
        if sys.platform == "win32" and "APPDATA" in os.environ:
            homedir = os.environ["APPDATA"]

        # Otherwise, install in the user's home directory.
        else:
            homedir = os.path.expanduser("~/")
            if homedir == "~/":
                raise ValueError("Could not find a default download directory")

        # append "nltk_data" to the home directory
        return os.path.join(homedir, "geosearch_data")

    @staticmethod
    def _is_writable(path):
        """
        Code borrowed from nltk (https://www.nltk.org/)
        """

        # Ensure that it exists.
        if not os.path.exists(path):
            return False

        # If we're on a posix system, check its permissions.
        if hasattr(os, "getuid"):
            statdata = os.stat(path)
            perm = stat.S_IMODE(statdata.st_mode)
            # is it world-writable?
            if perm & 0o002:
                return True
            # do we own it?
            elif statdata.st_uid == os.getuid() and (perm & 0o200):
                return True
            # are we in a group that can write to it?
            elif (statdata.st_gid in [os.getgid()] + os.getgroups()) and (perm & 0o020):
                return True
            # otherwise, we can't write to it.
            else:
                return False

        # Otherwise, we'll assume it's writable.
        # [xx] should we do other checks on other platforms?
        return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--download', action="store_true", help='Download geonames')
    parser.add_argument('--download_url', help="Geonames database url",
                        default="https://download.geonames.org/export/dump/allCountries.zip")
    parser.add_argument('--update', action="store_true",
                        help="Overwrites current database")
    parser.add_argument('--query', type=str, help='Query by name')
    parser.add_argument('--query_coord', type=str, help='Query by geocoordinates(lat, lon)')
    args = parser.parse_args()

    if args.download:
        GeoSearch.download(data_url=args.download_url, overwrite=not args.update)

    qresult = []

    if args.query_coord:
        gsearch = GeoSearch()
        lat, lon = map(float, args.query_coord.split(","))
        qresult = gsearch.query_coord(lat, lon, limit=10)

    if args.query:
        gsearch = GeoSearch()
        qresult = gsearch.query(args.query, limit=10)

    for sres in qresult:
        sres['place']['modification_date'] = str(sres['place']['modification_date'])
        print(json.dumps(sres, indent=4, default=str))
