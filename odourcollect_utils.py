# visit https://www.lfd.uci.edu/~gohlke/pythonlibs
# for win64 requirements for KEPLERGL (GDAL and FIONA)
import array

import pandas as pd
import os
from dotenv import load_dotenv


class OCMapConfig:
    """
    Returns the contents of <conf_name>.conf in <conf_folder> subdir.
    Intended for providing KeplerGL map configs previously stored in files.
    """
    def __init__(self, conf_folder='mapconf'):
        self.conf_folder = os.path.splitext(os.path.basename(conf_folder))[0]
        if os.path.isdir(self.conf_folder):
            pass
        else:
            raise FileNotFoundError(
                '\"{}\" should be an existing subfolder, but is missing. '
                'Ensure that it exists and contains at least one .conf file.'.format(self.conf_folder))

    def get(self, conf_name):
        # First, for security, strip off path separators and any bad intentioned stuff passed through confname
        conf_name = os.path.splitext(os.path.basename(conf_name))[0]
        # Next line should yield something like 'mapconf/mymap.conf'
        map_conf_path = os.path.join(self.conf_folder, '%s.conf' % conf_name)
        # Now... does it exist?
        if os.path.isfile(map_conf_path):
            with open(map_conf_path) as conf_file:
                conf_contents = conf_file.read()
                try:
                    return eval(conf_contents)  # Checks if valid dict or causes SyntaxError
                except SyntaxError as e:
                    raise SyntaxError('Contents of \"{}\" are not a valid dictionary'.format(map_conf_path)) from e
        else:
            raise FileNotFoundError(
                '\"{}.conf\" should be an existing file in \"{}\" subfolder, but is missing. Ensure that you specified '
                'the name of a valid config without extension.'.format(self.conf_folder, conf_name))


class GenericOCData:
    """
    Loads CSV/XLSX file to a Pandas DataFrame. Fully agnostically.
    OCObservationData and OCAnalysisData rely on it
    """
    def __init__(self, file_name, file_type='auto'):
        # Load CSV or XLSX
        # TODO: What if we need to load data directly from a Pandas.DataFrame? This architechture precludes to do so!
        self.file_name = file_name  # just for information purposes
        self.file_type = file_type
        self.data = pd.DataFrame
        if self.file_type == 'auto':
            self.file_type = os.path.splitext(file_name)[1].split('.', -1)[1]
            print('INFO: File type autodetection yields: {}'.format(self.file_type))
        if self.file_type == 'csv':
            self.data = pd.read_csv(file_name, na_filter=False, parse_dates=False, true_values='Yes',
                                    false_values='No', dtype=object)
        elif self.file_type == 'xlsx' or self.file_type == 'xls':
            # REMEMBER that this only reads the FIRST sheet of excel file!
            print('INFO: Remember that we only load data from 1st sheet of MS Excel Files')
            self.data = pd.read_excel(file_name, na_filter=False, parse_dates=False, true_values='Yes',
                                      false_values='No', dtype=object)
        else:
            raise AssertionError('file_type must be "csv" or "xlsx"')

    def to_excel(self, filename, **kwargs):
        print('Exporting data to file \"{}\"...'.format(filename))  # 1st version of this struct.
        self.data.to_excel(filename, **kwargs)

    class OCFilterProvider:
        """
        INNER/NESTED class inside DataLoader class, for storing and managing data prep routines (filters)
        just right after it has been loaded. It enables any children class to load and transform data automatically
        declaring a list of routines for data transformation from the set of routines stored in this class.
        """


        def __init__(self, data_frame, filter_list):
            """
            :type data_frame: pd.DataFrame
            :type filter_list: list
            """
            self.AVAILABLE = set([f for f in dir(self) if not f.startswith('_')])
            if not set(filter_list).issubset(self.AVAILABLE):
                # ALL filters specified HAVE to be available in this filter provider. Otherwise, raise Exception
                raise ValueError('The list of OC data filters specified includes at least one not known.'
                                 'Available filters:\n{}'.format(str(self.AVAILABLE)))
            else:
                self.filter_list = filter_list
                self.__run__(data_frame)

        def __run__(self, data_frame):
            """
            :type data_frame: pd.DataFrame
            :return: pd.DataFrame
            """
            for data_filter in self.filter_list:
                print('Running filters: \"{}\"...'.format(data_filter))
                data_frame = getattr(self, data_filter)(data_frame)
            print('Filter execution went OK')
            return data_frame

        @staticmethod
        def fix_typos(ocdata):
            # Detects the INTENTITY typo in OC's export files
            # Should be filter #1 in OCObservationData
            if 'Intentity' in ocdata:
                print('INFO: Correcting typo in column "Intensity" until OCs developers fix it someday ;-)')
                ocdata.rename(columns={'Intentity': 'Intensity'}, inplace=True)  # and correct it
            if 'day' in ocdata:
                ocdata.rename(columns={'day': 'Day'}, inplace=True)  # other minor typo
            if 'time' in ocdata:
                ocdata.rename(columns={'time': 'Time'}, inplace=True)  # another one
            return ocdata

        @staticmethod
        def fix_userids(ocdata):
            # Export files generated by non-superadmins don't include user ids.
            # This filter checks existence of column 'User'. If missing, injects a blank one to homogenize structure.
            # Should be filter #2 in OCObservationData
            if 'User' not in ocdata:
                print('INFO: Data does NOT include user ids. Generate files as superadmin if you need them.')
                ocdata['User'] = ""
            return ocdata

        @staticmethod
        def odour_literals_to_numbers(ocdata):
            # Odour intensity and annoyance conversion from literals to numbers
            # Should be filter #3 in OCObservationData (relies on #1)

            annoy_mapping = {"Extremely unpleasant": "-4",
                             "Very unpleasant": "-3",
                             "Unpleasant": "-2",
                             "Slightly unpleasant": "-1",
                             "Neutral": "0",
                             "Slightly pleasant": "1",
                             "Pleasant": "2",
                             "Very pleasant": "3",
                             "Extremely pleasant": "4",
                             "": "0"}  # Empty string should not be there, but just in case
            intensity_mapping = {"Extremely strong": "6",
                                 "Very strong": "5",
                                 "Strong": "4",
                                 "Distinct": "3",
                                 "Weak": "2",
                                 "Very weak": "1",
                                 "Not perceptible": "0",
                                 "": "0"}  # Empty string should not be there, but just in case

            # EXPERIMENTAL: Translating durations from simple categoric useless data to pseudo-timedelta (in minutes)
            duration_mapping = {"Continuous odor throughout the day": "1 day 00:00:00",
                                "Continuous odor in the last hour": "01:00:00",
                                "Punctual odor": "00:05:00",
                                "": "00:00:00"}
            intensity_and_annoy_mapping = {"Intensity": intensity_mapping,
                                           "Annoy": annoy_mapping,
                                           "Duration": duration_mapping}
            ocdata.replace(intensity_and_annoy_mapping, inplace=True)

            # And now, some bastards ;)
            # These are values that need to be detected/changed because of historic errors in OC.
            # "Extremely pleasan" (without t) is a FIX in older format.
            # "Moderate (un)pleasant" are similar cases. It's an historic mistake in literals used.
            bastard_tone_mapping = {"Moderate unpleasant": "-3",
                                    "Moderate pleasant": "3",
                                    "Extremely pleasan": "4"}
            ocdata.replace({"Annoy": bastard_tone_mapping}, inplace=True)
            # Finally, we can return the data corrected and prepared
            return ocdata

        @staticmethod
        def add_analyst_fields(ocdata):
            # If missing, adds extra columns for data analyst work (respecting original citizen's data)
            # The input for this filter should be OCObservationData (a regular OdourCollect observations export)
            # Should be filter #4 in OCObservationData
            for analystextrafield in ['Typeoverride', 'Subtypeoverride', 'Intensityoverride', 'Annoyoverride',
                                      'Analystcomments']:
                if analystextrafield in ocdata:
                    # It is not supposed for these columns to be already in OCObservationData data file structure.
                    raise AssertionError('The data provided already has extra fields for data analysts. That means that'
                                         ' it\'s not an observations file but an analysis file. Operation cancelled.')
                else:
                    print('Upgrading data structure for analysis. Adding \"{}\" field...'.format(analystextrafield))
                    ocdata[analystextrafield] = ''
            return ocdata

        @staticmethod
        def type_casting(ocdata):
            # Category type casting for the different types of data that may be in the data structure.
            # Should be convenient in both OCObservationData and OCAnalysisData data structures.
            # Intended to be very last filter to be run.
            category_types = ['Type', 'Subtype', 'Zone', 'Status', 'Origin', 'User', 'Intensityoverride',
                              'Annoyoverride', 'Duration']  # The xxxxoverride fields must allow null, so won't be int
            datetime_types = ['Day']
            timedelta_types = ['Time', 'Duration']
            int_types = ['Intensity', 'Annoy']
            for column in ocdata:
                if column in category_types:
                    ocdata[column] = ocdata[column].astype('category')
                if column in datetime_types:
                    ocdata[column] = pd.to_datetime(ocdata[column])
                if column in timedelta_types:
                    # This requires preliminary arrangements. Check odour_literals_to_numbers() in FilterProvider
                    ocdata[column] = pd.to_timedelta(ocdata[column])/pd.Timedelta('60s')
                    ocdata[column] = ocdata[column].round()
                if column in int_types:
                    # Yes, this is right. See https://github.com/pandas-dev/pandas/issues/25472
                    ocdata[column] = ocdata[column].str.replace(' ', '')
                    ocdata[column] = ocdata[column].astype(str)
                    ocdata[column] = ocdata[column].astype(int)

class OCObservationData(GenericOCData):
    def __init__(self, file_name, file_type='auto'):
        super().__init__(file_name, file_type)
        self.filters = self.OCFilterProvider(self.data, ['fix_typos', 'fix_userids', 'odour_literals_to_numbers',
                                                         'add_analyst_fields', 'type_casting'])


class OCAnalysisData(GenericOCData):
    def __init__(self, file_name, file_type='auto'):
        super().__init__(file_name, file_type)
        self.filters = self.OCFilterProvider(self.data, ['type_casting'])


if __name__ == '__main__':
    # If called directly, consider self-test
    load_dotenv()
    mapbox_api_key = os.getenv('MAPBOX_API_KEY')
    test_path = os.getenv('TEST_PATH')
    ocmap = OCObservationData(test_path)
