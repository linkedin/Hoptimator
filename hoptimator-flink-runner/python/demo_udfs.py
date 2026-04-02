from pyflink.table.udf import udf
from pyflink.table import DataTypes


@udf(result_type=DataTypes.STRING())
def reverse_string(s):
    """A simple Python UDF that reverses a string."""
    return s[::-1] if s else None
