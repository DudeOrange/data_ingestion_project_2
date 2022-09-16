
import pandas as pd
import de_project.tests.data_quality_functions as dqf

# length of string column in database table
length_of_string = dict(INCIDENT_DAY_OF_WEEK = 9, 
                        REPORT_TYPE_CODE = 2, 
                        REPORT_TYPE_DESCRIPTION = 19,
                        INCIDENT_CATEGORY = 44,
                        INCIDENT_SUBCATEGORY = 40,
                        INCIDENT_DESCRIPTION = 84,
                        RESOLUTION = 20,
                        INTERSECTION = 84,
                        POLICE_DISTRICT = 10,
                        ANALYSIS_NEIGHBORHOOD = 30,
                        POINT = 46)

#settled values in some columns
settled_columns = dict( 
    REPORT_TYPE_DESCRIPTION = ['Coplogic Initial', 'Coplogic Supplement', 'Initial', 'Initial Supplement', 'Vehicle Initial', 'Vehicle Supplement'],
    RESOLUTION = ['Cite or Arrest Adult', 'Exceptional Adult', 'Open or Active', 'Unfounded'],
    INCIDENT_DAY_OF_WEEK = ['Friday', 'Monday', 'Saturday', 'Sunday', 'Thursday', 'Tuesday', 'Wednesday'])

unique_columns = ['ROW_ID']

not_null_columns = ['INCIDENT_DATETIME', 'INCIDENT_DATE', 'INCIDENT_TIME', 'INCIDENT_YEAR', 'INCIDENT_DAY_OF_WEEK',
                    'REPORT_DATETIME', 'ROW_ID', 'INCIDENT_ID', 'INCIDENT_NUMBER', 'REPORT_TYPE_CODE', 'REPORT_TYPE_DESCRIPTION']



def data_quality_test(path):
    """
    Run data quality tests one by one. If test not pass, raised exception and data don't insert into database table.

    path - path to file that must be checked.
    """

    df = pd.read_csv(path)
    df.rename(columns=lambda x: x.replace(' ', '_').upper(), inplace=True)
    df.drop(columns = ['ESNCAG_-_BOUNDARY_FILE', 'CENTRAL_MARKET/TENDERLOIN_BOUNDARY_POLYGON_-_UPDATED', 
            'CIVIC_CENTER_HARM_REDUCTION_PROJECT_BOUNDARY', 'HSOC_ZONES_AS_OF_2018-06-05', 'INVEST_IN_NEIGHBORHOODS_(IIN)_AREAS'], inplace=True)

    dqf.expect_column_values_to_be_unique(df, unique_columns)

    dqf.expect_column_values_lengths_to_be_equal(df, length_of_string)

    dqf.expect_column_values_to_be_not_null(df, not_null_columns)

    dqf.expect_column_values_to_be_in_set(df, settled_columns)



