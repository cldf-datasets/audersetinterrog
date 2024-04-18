import collections
import pathlib
from itertools import islice

from cldfbench import CLDFSpec
from cldfbench import Dataset as BaseDataset
from clldutils.misc import slug
from pybtex.database import parse_string


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "audersetinterrog"

    def cldf_specs(self):
        return CLDFSpec(dir=self.cldf_dir, module="StructureDataset")

    def cmd_makecldf(self, args):

        # read raw data

        parameters = list(self.etc_dir.read_csv("parameters.csv", dicts=True))
        for parameter in parameters:
            if (grammacodes := parameter.get('Grammacodes')):
                parameter['Grammacodes'] = [
                    id_.strip()
                    for id_ in grammacodes.split(',')]
        codes_by_id = collections.OrderedDict(
            (row['ID'], row)
            for row in self.etc_dir.read_csv('codes.csv', dicts=True))

        raw_data = list(self.raw_dir.read_csv(
            "InterrogativeRelativeIE_Appendix1.csv", dicts=True))

        sources = parse_string(self.raw_dir.read('AudersetReferences.bib'), 'bibtex')

        language_sources = {
            lang_id: [
                stripped
                for source in sources.split(',')
                if (stripped := source.strip())]
            for lang_id, sources in islice(
                self.etc_dir.read_csv('language-sources.csv'),
                1, None)}

        value_sources = {
            (lang_id, param_id): {
                'Comment': comment,
                'Source': [
                    stripped
                    for source in sources.split(';')
                    if (stripped := source.strip())],
            }
            for lang_id, param_id, comment, sources in islice(
                self.etc_dir.read_csv('value-sources.csv'),
                1, None)}

        # knead into a cldfifiable shape

        languages_by_id = collections.OrderedDict()
        for row in raw_data:
            if row['Glottocode'] not in languages_by_id:
                languages_by_id[row['Glottocode']] = {
                    'ID': row['Glottocode'],
                    'Glottocode': row['Glottocode'],
                    'Name': row['Language'],
                    'Family': row['Branch'],
                    'Subbranch': row['Subbranch'],
                    'Subsubbranch': row['Subsubbranch'],
                    'EarlyTimeBP': row['EarlyTimeBP'],
                    'LateTimeBP': row['LateTimeBP'],
                    'AvTimeBP': row['AvTimeBP'],
                    'Latitude': row['Latitude'],
                    'Longitude': row['Longitude'],
                    'Source': language_sources.get(row['Glottocode'], ()),
                }

        constructions = [
            {
                'ID': row['ID'],
                'Name': '{} relative pronoun {}'.format(
                    languages_by_id[row['Glottocode']]['Name'],
                    row['RMform']),
                'Language_ID': row['Glottocode'],
            }
            for row in raw_data]

        cvalues = []
        for row in raw_data:
            for parameter in parameters:
                if parameter['ID'] == 'rmforms':
                    # this one is generated later
                    continue
                elif parameter['ID'] == 'rmform':
                    code_id = None
                    comment = row['Notes']
                else:
                    code_id = '{}-{}'.format(
                        parameter['ID'], slug(row[parameter['Sheet_Column']]))
                    comment = None
                cvalue = {
                    'ID': '{}-{}'.format(row['ID'], parameter['ID']),
                    'Construction_ID': row['ID'],
                    'Parameter_ID': parameter['ID'],
                    'Value': row[parameter['Sheet_Column']],
                    'Code_ID': code_id,
                    'Comment': comment,
                }
                cvalues.append(cvalue)

        forms_by_language = collections.OrderedDict()
        for row in raw_data:
            glottocode = row['Glottocode']
            if glottocode not in forms_by_language:
                forms_by_language[glottocode] = []
            forms_by_language[glottocode].append(row['RMform'])
        values = [
            {
                'ID': f'rmforms-{lang_id}',
                'Language_ID': lang_id,
                'Parameter_ID': 'rmforms',
                'Value': ' / '.join(forms),
                'Source':
                    value_sources
                    .get((lang_id, 'rmforms'), {})
                    .get('Source', ()),
                'Comment':
                    value_sources
                    .get((lang_id, 'rmforms'), {})
                    .get('Comment', ''),
            }
            for lang_id, forms in forms_by_language.items()]

        # cldf output

        args.writer.cldf.add_component(
            "LanguageTable",
            "Family",
            "Subbranch",
            "Subsubbranch",
            "EarlyTimeBP",
            "LateTimeBP",
            "AvTimeBP",
            'http://cldf.clld.org/v1.0/terms.rdf#source',
        )
        args.writer.cldf.add_component(
            "ParameterTable",
            {
                'name': 'Grammacodes',
                'separator': ';',
            })
        args.writer.cldf.add_component("CodeTable")
        args.writer.cldf.add_table(
            'constructions.csv',
            'http://cldf.clld.org/v1.0/terms.rdf#id',
            'http://cldf.clld.org/v1.0/terms.rdf#languageReference',
            'http://cldf.clld.org/v1.0/terms.rdf#name',
            'http://cldf.clld.org/v1.0/terms.rdf#description')
        args.writer.cldf.add_table(
            'cvalues.csv',
            'http://cldf.clld.org/v1.0/terms.rdf#id',
            'Construction_ID',
            'http://cldf.clld.org/v1.0/terms.rdf#parameterReference',
            'http://cldf.clld.org/v1.0/terms.rdf#codeReference',
            'http://cldf.clld.org/v1.0/terms.rdf#value',
            'http://cldf.clld.org/v1.0/terms.rdf#comment')

        args.writer.cldf.add_foreign_key(
            'cvalues.csv', 'Construction_ID',
            'constructions.csv', 'ID')

        args.writer.objects['LanguageTable'] = languages_by_id.values()
        args.writer.objects['constructions.csv'] = constructions
        args.writer.objects['ParameterTable'] = parameters
        args.writer.objects['CodeTable'] = codes_by_id.values()
        args.writer.objects['cvalues.csv'] = cvalues
        args.writer.objects['values.csv'] = values

        args.writer.cldf.add_sources(sources)
