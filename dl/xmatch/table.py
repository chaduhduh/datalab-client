import os
from typing import Callable
from .exceptions import XMatchException


class TableTypes:
    """
    Utility methods to work with different Data Lab table types: mydb, vos,
    tapdb and csv_file
    """
    MYDB = "mydb"
    VOS = "vos"
    TAPDB = "tap"
    CSV_FILE = "csv_f"

    @staticmethod
    def detect_type(table_name: str=""):
        """
        For a given table name/query target, determine the "type"
        of resource the user is attempting to query.
        """
        table_type = TableTypes.TAPDB
        if "mydb://" in table_name:
            table_type = TableTypes.MYDB
        elif "vos://" in table_name:
            table_type = TableTypes.VOS
        elif table_name[0] == "/" or ".csv" in table_name.lower():
            table_type = TableTypes.CSV_FILE
        return table_type

    @classmethod
    def base_name(cls, table_name: str=""):
        """
        Return the base name of the table as determined by the type of table
        """
        base = table_name
        table_type = cls.detect_type(table_name)
        if table_type == cls.MYDB:
            base = table_name.replace("mydb://", "")
        elif table_type == cls.VOS:
            base = table_name.replace("vos://", "")
        elif table_type == cls.CSV_FILE:
            _, file = os.path.split(table_name)
            base = file
        return base


class XMatchTable():
    """
    Define and configure a cross match table for performing various types
    of crossmatches
    """
    def __init__(self, table_name: str, output_cols: list=None,
                 ra: str="ra", dec: str="dec", import_name: str="",
                 error_circle: str=None, major_axis:str =None,
                 axis_ratio: str=None, pos_angle: str=None
                 ):
        self.name = table_name
        self.output_cols = output_cols if output_cols else ["all"]
        self.table_type = TableTypes.detect_type(table_name)
        self.ra = ra
        self.dec = dec
        self.import_name = import_name
        self.error_circle = error_circle
        self.major_axis = major_axis
        self.axis_ratio = axis_ratio
        self.pos_angle = pos_angle

    def set_name(self, name):
        """
        Update the table name and reload any computed attributes
        """
        self.name = name
        self.table_type = TableTypes.detect_type(self.name)

    def alias(self):
        """
        return a suitable alias for the provided table name
        """
        delimeter = "__"
        t_symbol = "."
        if self.table_type in [TableTypes.MYDB, TableTypes.VOS]:
            t_symbol = "://"
        return self.name.replace(t_symbol, delimeter)

    def prepare(self, csv_file: Callable=lambda x: None,
                vos_file: Callable=lambda x: None):
        """ prepare tables for cross match as needed """
        #TODO: using an import_name param simplifies the implementation
        #       we could probably make this automatic but might need temp
        #       tables or a numbered table system and might leave many
        #       unwanted artifacts
        #TODO: we don't want to wrap all of our import features, we should
        #      consider this a "simple" import and favor our normal table
        #      preparation tools for more complex import tasks
        prepare_call = None
        if self.table_type == TableTypes.CSV_FILE:
            prepare_call = csv_file
        if self.table_type == TableTypes.VOS:
            prepare_call = vos_file

        if prepare_call:
            try:
                csv_file()
            except Exception as exc:
                raise XMatchException((
                    "Error encoutered during the import process of "
                    f"{self.name}. Ensure that the file exists and "
                    f"the mydb table name '{self.import_name}' isn't taken"
                )) from exc
            self.set_name(f"mydb://{self.import_name}")

    def request_format(self):
        """
        Convert an XMatch table instance to request format (dict)
        """
        return dict(
            table_name=self.name,
            output_cols=self.output_cols,
            ra=self.ra,
            dec=self.dec,
            import_name=self.import_name,
            error_circle=self.error_circle,
            major_axis=self.major_axis,
            axis_ratio=self.axis_ratio,
            pos_angle=self.pos_angle
        )

    def use_all_cols(self):
        """
        Returns True if the table should output all columns such as tables that
        don't specify output columns or have output columns set to ['all]
        """
        return self.output_cols[0].lower() == 'all'

    def base_name(self):
        """
        Return the base name of the table, logic varies depending on the type
        of table
        """
        return TableTypes.base_name(self.name)
