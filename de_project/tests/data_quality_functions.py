

def expect_column_values_to_be_unique(dataframe, columns):
    """
    Checking uniqueness of the column.

    columns - list(string) name of column, that must be checked.
    """
    for col in columns:
        assert dataframe[col].nunique() == len(dataframe[col]), f'Values in column {col} is not unique'


def expect_column_values_lengths_to_be_equal(dataframe, columns_lengths):
    """
    Check that length of string column in dataframe less or equal length of column in database table.

    columns_lengths - dictonary(string, int). Keys: name of column. Values: length of column.
    """

    for col in columns_lengths.keys():
        assert dataframe[col].map(lambda x: len(str(x))).max() <= columns_lengths[col], f'Values in column {col} is too long'


def expect_column_values_to_be_not_null(dataframe, columns):
    """
    Check that column not contain null values.

    columns - list(string) name of column, that must be checked.
    """

    for col in columns:
        assert dataframe[col].isnull().sum() == 0, f'Column {col} contain null values'


def expect_column_values_to_be_in_set(dataframe, set_columns):
    """
    Check that column contain only settled values.

    set_columns - dictonary(string, list(string)). Keys: name of column. Values: list of settled values.
    """

    for col in set_columns:
        assert sorted(dataframe[col].unique()) == set_columns[col], f'Column {col} contain not settled values'


